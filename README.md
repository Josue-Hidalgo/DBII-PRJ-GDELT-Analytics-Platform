# DBII-PRJ-GDELT-Analytics-Platform
GDELT Analytics Platform es una plataforma que recopila datos de eventos mundiales desde GDELT cada 15 minutos, los procesa mediante Apache Spark para generar métricas y análisis, almacena los resultados en MongoDB y los presenta en un dashboard interactivo, permitiendo visualizar tendencias, conflictos y patrones globales de forma automatizada.

## Autores

- [@Josue-Hidalgo](https://www.github.com/Josue-Hidalgo)
- [@DayRPK25](https://github.com/DayRPK25)
- [@IanUgaldeTec](https://github.com/IanUgaldeTec)
- [@PauloHerrera1](https://github.com/PauloHerrera1)

<img width="1254" height="1254" alt="Logo" src="https://github.com/user-attachments/assets/a9041cdf-9ab5-4275-8d20-50a8b714b018" />

## Instalación Dependencias
Antes de ejecutar el proyecto, asegúrese de tener instaladas las siguientes herramientas:

* Docker Engine 24 o superior
* Docker Compose v2 o superior
* Git

### Docker
- [Instalar Docker Engine](https://docs.docker.com/engine/install/)

Verifique la instalación con los siguientes comandos:

```bash
docker --version
docker compose version
git --version
```

## Clonar el Repositorio

```bash
git clone https://github.com/Josue-Hidalgo/DBII-PRJ-GDELT-Analytics-Platform.git
cd DBII-PRJ-GDELT-Analytics-Platform
```

## Levantar los Contenedores

Construir y ejecutar todos los servicios:

```bash
docker compose up --build -d
```

Verificar que los contenedores estén ejecutándose:

```bash
docker ps
```

## Detener los Contenedores

```bash
docker compose down
```

## Reiniciar el Proyecto

```bash
docker compose down
docker compose up --build -d
```

## Apagar y Eliminar Volúmenes
Este comando elimina los contenedores y los volúmenes persistentes asociados al proyecto.
```bash
docker compose down -v
```

## Documentación

 - [Documentación Externa](https://docs.google.com/document/d/1nAHviEI59KieZjGjtOoCA8HmAS-5ed8zgQPaiDaC_u4/edit?usp=sharing)
