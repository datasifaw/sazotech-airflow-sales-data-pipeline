\# 🚀 Sazotech Airflow Sales Data Pipeline



Projet Data Engineering mettant en place un pipeline ETL automatisé permettant d'extraire des données commerciales depuis Microsoft SQL Server, de les transformer avec Apache Airflow, de les charger dans PostgreSQL puis de les visualiser dans Metabase.



\## 🎯 Objectif du projet



L'objectif est de construire une chaîne de traitement de données complète et reproductible :



\*\*SQL Server → Apache Airflow → PostgreSQL → Metabase\*\*



Le pipeline automatise l'extraction, la transformation, le chargement et la mise à disposition des données pour leur analyse.



\## 🏗️ Architecture



```text

┌─────────────────────┐

│ Microsoft SQL Server│

│ Base : Ventes       │

└──────────┬──────────┘

&#x20;          │

&#x20;          │ Extraction

&#x20;          ▼

┌─────────────────────┐

│   Apache Airflow    │

│                     │

│  extract\_sales      │

│       ↓             │

│  transform\_sales    │

│       ↓             │

│  load\_postgres      │

│       ↓             │

│  check\_data         │

└──────────┬──────────┘

&#x20;          │

&#x20;          │ Chargement

&#x20;          ▼

┌─────────────────────┐

│     PostgreSQL      │

│   Data Warehouse    │

│     Sales Fact      │

└──────────┬──────────┘

&#x20;          │

&#x20;          │ Analyse

&#x20;          ▼

┌─────────────────────┐

│      Metabase       │

│                     │

│ Dashboard commercial│

└─────────────────────┘

```



\## 🛠️ Technologies utilisées



\- \*\*Microsoft SQL Server\*\* : base de données source

\- \*\*Apache Airflow\*\* : orchestration du pipeline ETL

\- \*\*PostgreSQL\*\* : stockage des données transformées

\- \*\*Metabase\*\* : visualisation et analyse des données

\- \*\*Docker / Docker Compose\*\* : conteneurisation de l'environnement

\- \*\*Python\*\* : développement du pipeline

\- \*\*Git / GitHub\*\* : gestion de versions



\## 🔄 Fonctionnement du pipeline ETL



Le DAG principal est :



```text

sazotech\_sales\_pipeline

```



Il contient quatre étapes principales :



```text

extract\_sales

&#x20;     ↓

transform\_sales

&#x20;     ↓

load\_postgres

&#x20;     ↓

check\_data

```



\### 1. Extraction — `extract\_sales`



Airflow se connecte à Microsoft SQL Server et récupère les données commerciales provenant de la base `Ventes`.



\### 2. Transformation — `transform\_sales`



Les données extraites sont préparées et transformées afin de produire un dataset adapté à l'analyse.



Les informations exploitées comprennent notamment :



\- identifiant de commande ;

\- client ;

\- société ;

\- ville ;

\- pays ;

\- date de commande ;

\- produit ;

\- quantité vendue ;

\- chiffre d'affaires.



\### 3. Chargement — `load\_postgres`



Les données transformées sont chargées dans PostgreSQL afin de disposer d'une base analytique indépendante de la base SQL Server source.



\### 4. Contrôle — `check\_data`



La dernière tâche du DAG vérifie que les données ont correctement été chargées dans PostgreSQL.



\## 📊 Dashboard Metabase



Metabase est connecté à PostgreSQL afin de construire un dashboard commercial à partir des données générées par le pipeline.



Le dashboard \*\*Sazotech Sales Dashboard\*\* permet notamment de suivre :



\- le chiffre d'affaires total ;

\- la quantité totale vendue ;

\- le chiffre d'affaires par pays ;

\- les principaux clients par chiffre d'affaires ;

\- les principaux produits par chiffre d'affaires ;

\- l'évolution du chiffre d'affaires dans le temps.



Exemple de KPI obtenu :



```text

Chiffre d'affaires total : environ 6,5 M

Quantité totale vendue   : 239 417

```



\## 🐳 Architecture Docker



L'environnement utilise Docker Compose avec les principaux services suivants :



```text

airflow

postgres

metabase

```



Ports utilisés en local :



| Service | Port |

|---|---:|

| Apache Airflow | 8084 |

| Metabase | 3001 |

| PostgreSQL | 5434 |

| SQL Server source | 1433 |



\## 📁 Structure du projet



```text

sazotech-airflow-sales-data-pipeline/

│

├── dags/

│   └── sales\_pipeline.py

│

├── postgres/

│

├── .env.example

├── .gitignore

├── docker-compose.yml

├── Dockerfile

├── requirements.txt

└── README.md

```



\## ⚙️ Installation



\### Prérequis



Avant de démarrer le projet, installer :



\- Docker Desktop

\- Git

\- Microsoft SQL Server avec la base source

\- PowerShell ou un terminal équivalent



\### 1. Cloner le projet



```bash

git clone https://github.com/datasifaw/sazotech-airflow-sales-data-pipeline.git

cd sazotech-airflow-sales-data-pipeline

```



\### 2. Créer le fichier `.env`



Copier le fichier d'exemple :



```powershell

Copy-Item .env.example .env

```



Puis renseigner les paramètres de connexion SQL Server dans `.env`.



Exemple :



```env

MSSQL\_HOST=host.docker.internal

MSSQL\_PORT=1433

MSSQL\_DB=Ventes

MSSQL\_USER=votre\_utilisateur

MSSQL\_PASSWORD=votre\_mot\_de\_passe

```



> Le fichier `.env` contient des informations sensibles et ne doit pas être envoyé sur GitHub.



\### 3. Construire l'image Airflow



```powershell

docker compose build

```



\### 4. Démarrer l'environnement



```powershell

docker compose up -d

```



\### 5. Vérifier les conteneurs



```powershell

docker compose ps

```



Les services Airflow, PostgreSQL et Metabase doivent être actifs.



\## 🌐 Interfaces



Après le démarrage des conteneurs :



\### Apache Airflow



```text

http://localhost:8084

```



Le DAG à utiliser est :



```text

sazotech\_sales\_pipeline

```



\### Metabase



```text

http://localhost:3001

```



Metabase permet d'explorer les données PostgreSQL et d'accéder au dashboard commercial.



\## 🔌 Connexion SQL Server dans Airflow



Le pipeline utilise une connexion Airflow de type Microsoft SQL Server.



Identifiant de connexion :



```text

mssql\_ventes

```



Configuration principale :



```text

Host     : host.docker.internal

Port     : 1433

Database : Ventes

```



Les identifiants réels doivent être configurés localement et ne doivent pas être stockés directement dans le dépôt Git.



\## ✅ Vérification du pipeline



Pour vérifier que le DAG est détecté :



```powershell

docker compose exec airflow airflow dags list

```



Pour consulter les exécutions :



```powershell

docker compose exec airflow airflow dags list-runs -d sazotech\_sales\_pipeline

```



Une exécution réussie doit apparaître avec l'état :



```text

success

```



\## 🔄 Redémarrage après extinction du PC



Après avoir redémarré Windows :



1\. Démarrer Docker Desktop.

2\. Ouvrir PowerShell.

3\. Se placer dans le projet :



```powershell

cd C:\\PROJET\_AIRFLOW\\sazotech-airflow-sales-data-pipeline

```



4\. Démarrer les conteneurs :



```powershell

docker compose up -d

```



5\. Vérifier leur état :



```powershell

docker compose ps

```



Airflow et Metabase seront ensuite de nouveau accessibles sur leurs ports respectifs.



\## 🔐 Sécurité



Les mots de passe et autres secrets ne doivent jamais être stockés directement dans Git.



Le fichier `.env` est exclu du dépôt grâce au fichier `.gitignore`.



Le dépôt fournit uniquement `.env.example` afin de documenter les variables nécessaires au fonctionnement du projet.



\## 📈 Résultat



Ce projet met en œuvre une chaîne Data Engineering complète :



```text

Source transactionnelle

&#x20;       ↓

Microsoft SQL Server

&#x20;       ↓

Extraction / Transformation

&#x20;       ↓

Apache Airflow

&#x20;       ↓

PostgreSQL

&#x20;       ↓

Metabase

&#x20;       ↓

Dashboard analytique

```



Le résultat est un pipeline automatisé et conteneurisé permettant de transformer des données commerciales brutes en indicateurs exploitables dans un outil de Business Intelligence.



\## 👤 Auteur



\*\*Sazotech\*\*



Projet Data Engineering — Apache Airflow / SQL Server / PostgreSQL / Docker / Metabase

