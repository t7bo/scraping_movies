# Scraping de Films et Séries depuis IMDb

## Objectif

L'objectif de ce projet est de collecter des données sur les films et les séries à partir du site IMDb en utilisant des techniques de scraping avec Python. Les informations collectées peuvent inclure les titres, les notes, les acteurs principaux, les genres, les années de sortie, etc. Ces données seront collectées, nettoyées pour ensuite être stockées dans plusieurs bases de données SQLite pour une utilisation ultérieure.

## Technologies utilisées

- Python
- Scrapy
- SQLite

## Structure du Projet

- `data`: Le dossier contenant les données scrappées sauvegardées sous format CSV ainsi que les bases de données créées.
- `moviescraper`: Dossier principal contenant les spiders qui ont permis de scrapper les données.