---
name: GCP VM fetch protocol
description: Protocolo para aprovisionar VM en São Paulo, ejecutar fetch remoto, y traer los datos. Replicable para cualquier país con fetch lento.
type: reference
originSessionId: e9f2681b-030b-4f00-9e82-43723731d6d3
---
# Protocolo: fetch remoto via GCP VM (São Paulo)

Para países cuyo servidor está en Sudamérica (AR, UY, CL, BR) y el fetch desde Europa es lento.

## 1. Provisionar VM

```bash
gcloud compute instances create legalize-fetch-{code} \
  --project=boletinclaro \
  --zone=southamerica-east1-b \
  --machine-type=e2-small \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --boot-disk-size=20GB
```

- **Proyecto**: `boletinclaro` (cuenta GCP de Enrique)
- **Zona**: `southamerica-east1-b` (São Paulo, ~30ms a Buenos Aires)
- **Máquina**: `e2-small` (2 vCPU, 2 GB RAM) — suficiente si el catálogo usa cache compartido
- **Coste**: ~€0.02/h, borrar cuando termine

## 2. Subir código + catálogo

```bash
cd ~/projects/legalize/engine

# Empaquetar el engine (sin fixtures, sin tests, solo src + config)
tar czf /tmp/{code}-engine.tar.gz src/legalize/ config.yaml pyproject.toml

# Subir engine
gcloud compute scp /tmp/{code}-engine.tar.gz legalize-fetch-{code}:~ --zone=southamerica-east1-b

# Subir catálogos pre-descargados (si existen)
gcloud compute scp ../countries/data-{code}/catalog/*.zip legalize-fetch-{code}:~ --zone=southamerica-east1-b
```

## 3. Configurar VM (SSH + instalar deps)

```bash
gcloud compute ssh legalize-fetch-{code} --zone=southamerica-east1-b

# En la VM:
sudo apt-get update -q && sudo apt-get install -y -q python3 python3-pip python3-venv screen
mkdir -p engine && cd engine
tar xzf ~/{code}-engine.tar.gz
python3 -m venv .venv && source .venv/bin/activate
pip install -q lxml requests pyyaml click rich
mkdir -p ../countries/data-{code}/catalog ../countries/data-{code}/json ../countries/{code}

# Copiar catálogos (si se subieron en paso 2)
cp ~/base-*.zip ../countries/data-{code}/catalog/ 2>/dev/null || true

# Verificar que importa OK
PYTHONPATH=src python3 -c "from legalize.config import load_config; print('OK')"
```

## 4. Lanzar fetch en screen (persistente)

```bash
# Crear script de fetch
cat > ~/run-fetch.sh << 'SCRIPT'
#!/bin/bash
cd ~/engine
source .venv/bin/activate
export PYTHONPATH=src
python3 -u -c "
from legalize.config import load_config
from legalize.pipeline import generic_fetch_all
config = load_config('config.yaml')
fetched = generic_fetch_all(config, '{code}', force=False)
print(f'DONE: {len(fetched)} norms fetched')
" > /tmp/fetch.log 2>&1
SCRIPT
chmod +x ~/run-fetch.sh

# Lanzar en screen y salir
screen -dmS fetch bash ~/run-fetch.sh
exit
```

Puedes cerrar SSH y apagar el Mac. La VM sigue.

## 5. Monitorear (desde local, puntualmente)

```bash
gcloud compute ssh legalize-fetch-{code} --zone=southamerica-east1-b --command='
  jsons=$(ls ~/countries/data-{code}/json/*.json 2>/dev/null | wc -l)
  tail -3 /tmp/fetch.log
  echo "$jsons JSONs"
  free -h | head -2
'
```

## 6. Traer los JSONs cuando termine

```bash
# En la VM: comprimir JSONs
gcloud compute ssh legalize-fetch-{code} --zone=southamerica-east1-b --command='
  tar czf /tmp/data-{code}-jsons.tar.gz -C ~/countries data-{code}/json/
'

# Desde local: descargar
gcloud compute scp legalize-fetch-{code}:/tmp/data-{code}-jsons.tar.gz /tmp/ --zone=southamerica-east1-b

# Extraer en la ubicación correcta
cd ~/projects/legalize/countries
tar xzf /tmp/data-{code}-jsons.tar.gz
```

## 7. Commit local (fast-import, minutos)

```bash
cd ~/projects/legalize/engine
rm -rf ../countries/{code} && mkdir -p ../countries/{code}
legalize commit -c {code} --all
```

O si el país tiene bootstrap hook custom:
```bash
legalize bootstrap -c {code}
```

## 8. Push + DB sync + deploy

```bash
# Push
git -C ../countries/{code} remote add origin git@github.com:legalize-dev/legalize-{code}.git 2>/dev/null
git -C ../countries/{code} branch -M main
git -C ../countries/{code} push -u origin main --force

# DB sync (repo local, no API)
cd ~/projects/legalize/enrichment
export DATABASE_URL=$(grep DATABASE_URL ~/projects/legalize/web/.env.production.local | head -1 | sed 's/^DATABASE_URL="//;s/"$//')
law-sync full --repo ~/projects/legalize/countries/{code}

# Web deploy
cd ~/projects/legalize/web
vercel deploy --prod --force
```

## 9. Borrar VM

```bash
gcloud compute instances delete legalize-fetch-{code} --zone=southamerica-east1-b --quiet
```

## Gotchas

- **RAM**: el catálogo InfoLEG ocupa ~250 MB en memoria. Con cache compartido (`_CATALOG_CACHE` en client.py), 4 workers caben en 2 GB. Sin cache, cada worker carga su copia → OOM.
- **Screen**: usar siempre `screen -dmS fetch bash script.sh` para que el proceso sobreviva al cierre de SSH. `nohup` dentro de SSH inline no persiste.
- **SSH timeout**: comandos SSH largos (>30s) pueden fallar. Partir en pasos cortos o subir scripts como archivos.
- **uv.lock**: si Vercel usa `uv` para instalar deps, regenerar `uv.lock` con `uv lock` después de cambiar `pyproject.toml`. Sin esto → `ModuleNotFoundError` en producción.
- **Catálogo mensual**: InfoLEG regenera el catálogo el día 1. Si quieres datos frescos, borrar `data-ar/catalog/*.zip` antes del fetch.

## Lección aprendida (2026-04-13, AR bootstrap)

**La VM en São Paulo reduce latencia 3× (300ms→107ms) pero NO elimina los timeouts del servidor InfoLEG.** El servidor Apache 2.2.22 da `Read timed out (60s)` independientemente de la ubicación del cliente. La VM ayuda para Tier 2 (30K normas × 1 request = 3h vs 9h), pero para Tier 1 (2K normas × 30 requests con timeouts) la mejora es marginal.

**Conclusión**: la VM vale la pena para países con muchas normas Tier 2 (1 request cada una). Para países donde la mayoría son Tier 1 (muchas modificatorias), el cuello de botella es el servidor, no la red.

## Multi-VM: partir el fetch entre varias máquinas

Para países con rate limit por IP, se puede usar N máquinas con IPs distintas.

### Ejemplo: 3 VMs para 32K normas

```bash
# VM-1: normas 0-10724
legalize fetch -c ar --all --limit 10725

# VM-2: normas 10725-21449
legalize fetch -c ar --all --offset 10725 --limit 10725

# VM-3: normas 21450-32175
legalize fetch -c ar --all --offset 21450
```

### Juntar los JSONs

Cada VM produce JSONs en `data-ar/json/`. Como los filenames son el `id_norma` (único), se pueden mezclar sin conflicto:

```bash
# Desde local:
for vm in vm1 vm2 vm3; do
  gcloud compute scp $vm:/tmp/data-ar-jsons.tar.gz /tmp/data-ar-$vm.tar.gz --zone=southamerica-east1-b
  tar xzf /tmp/data-ar-$vm.tar.gz -C ~/projects/legalize/countries/
done
# Todos los JSONs quedan en data-ar/json/ (merge automático por filename)
```

### Commit una sola vez

```bash
cd ~/projects/legalize/engine
legalize commit -c ar --all    # fast-import, lee TODOS los JSONs de data-ar/json/
```

### Notas

- El `discovery_ids.txt` se genera en la primera VM. Copiarlo a las demás para que usen el mismo orden y `--offset` funcione correctamente.
- Cada VM necesita el catálogo (`catalog/*.zip`). Subir una vez y copiar a todas.
- Los JSONs son independientes entre sí — no hay estado compartido.
