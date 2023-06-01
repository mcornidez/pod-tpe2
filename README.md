# Programación de Objetos Distribuidos
# Trabajo Práctico Especial 2: Alquiler de Bicicletas

## Compilación

Para compilar el proyecto, se debe ejecutar el siguiente comando en la raíz del proyecto:

```bash
mvn clean install
```

## Correr el servidor

Dentro de la carpeta /pod-tpe2-g2, correr el script:

```bash
./run-server.sh
```

## Ejecutar las queries

En otra terminal, dentro de la carpeta /pod-tpe2-g2, correr el script:

```bash
./queryX -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX-DoutPath=YY [params]
```

donde:
  ●  queryX es el script que corre la query X (1 o 2)
  ● -Daddresses refiere a las direcciones IP de los nodos con sus puertos (una o más,separadas por punto y coma)
  ● -DinPath indica el path donde están los archivos de entrada bikes.csv y stations.csv.
  ● -DoutPath indica el path donde estarán ambos archivos de salida query1.csv ytime1.txt.
  ● [params]: los parámetros extras que corresponden para algunas queries

  
## Query 1: Total de viajes iniciados por miembros por estación

❏ Parámetros adicionales: Ninguno

❏ Ejemplo de invocación: 

```bash
./query1 -Daddresses='10.6.0.1:5701' -DinPath=.-DoutPath=.
```

## Query 2: Top N viajes más rápidos de cada estación de inicio

❏ Parámetros adicionales: n límite de cantidad de resultados (número entero)

❏ Ejemplo de invocación: 

```bash
./query2 -Daddresses='10.6.0.1:5701' -DinPath=.-DoutPath=. -Dn=4
```
