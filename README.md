# Projet PageRank 2024-2025

Autheurs :
- BLAUDEAU Matthieu
- BROSSARD Victor
- HAJJI Amin

## Description du Projet
Ce projet, réalisé dans le cadre du cours **Gestion des données distribuées à large échelle** de **Pascal Molli**, a pour objectif d'évaluer les performances de l'algorithme PageRank en comparant deux implémentations :
- **PySpark DataFrame**
- **PySpark RDD**

## Objectifs
1. Comparer les performances entre **DataFrame** et **RDD** avec et sans partitionnement.
2. Tester différentes configurations de clusters :
   - 1 nœud
   - 2 nœuds
   - 4 nœuds  
   (en maintenant une parité matérielle : CPU/RAM par nœud constante).
3. Identifier l'entité avec le **plus grand score de PageRank**.

## Implémentation
Nous nous sommes inspiré du code disponible sur ce GitHub :  
[https://github.com/momo54/large_scale_data_management](https://github.com/momo54/large_scale_data_management)

## Résultats

Pour lancer les scripts bash, il faut spécifier en premier argument l'id du projet. Les résultats de temps s'affichent alors dans le terminal (entre les autres outputs)

Le temps total d'exécution du programme python pagerank_dataframe est de 99s.
Pour ce qui est de l'autre algorithme, nous n'avons pas réussi à résoudre un problème qui nous a empêché d'obtenir les informations attendues.