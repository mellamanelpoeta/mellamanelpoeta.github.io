---
layout: page
title: movies-info
description: Data pipeline using a movies database API.
img: assets/img/data-pipeline_pp.png
importance: 1
category: github
related_publications: 
---
[Github Repository](https://github.com/mellamanelpoeta/movies-info)

The objective of the project is to integrate the following elements:

+ Make requests to an API of our choice.
+ Insert the responses to the requests into a MongoDB database, which will function as a DataLake.
+ From the DataLake, it should be possible to make queries and perform an extraction, transformation, and loading process into two other databases: Cassandra and Neo4j.
+ Incorporate all elements within a Docker-Compose.
+ Throughout this document, the chosen API will be explained, how the project works, the requirements for its execution in any local environment, and some queries for each of the databases, along with 
 an explanation of the results they yield.

![image](https://www.themoviedb.org/assets/2/v4/logos/v2/blue_long_2-9665a76b1ae401a510ec1e0ca40ddcb3b0cfe45f1d51b77a308fea0845885648.svg)
## About the API: TMDB - The Movie Database API

[The Movie Database (TMDB)](https://www.themoviedb.org/) is an online database that contains a wide range of information about movies and TV shows. It provides details such as cast, production crew, release dates, synopses, ratings, user and critic scores, images, trailers, posters, and more. On the platform, users can contribute by adding information, correcting errors, or adding new content. This allows the database to stay up-to-date and accurate thanks to the participation of the user community. Additionally, users can rate movies and TV shows, helping to generate scores that reflect the overall community opinion about a particular title.

In addition to being a consumer website, TMDb provides an API that allows developers to access its vast database to create applications, websites, and entertainment-related services. To access the API, registration on the platform is required, obtaining a key or token. The key is obtained for free, subject to platform authorization. The API documentation can also be reviewed on their site.




## Initialization
To run this project on any computer, [Docker](https://www.docker.com/get-started/) must be installed beforehand. Follow the instructions below:
+ Clone this repository.
+ Ensure that the Docker daemon is running.
+ Navigate to the cloned repository's folder and run the run.sh file in the terminal:
```shell
./run.sh
```
  + In this file, Docker-Compose is executed, which loads the containers and opens VSCode. Open the movie_app.ipynb file in VSCode.
  + Run the notebook to visualize the results.

Queries for different databases should be executed in the terminal and can be found in the [Queries](https://github.com/Thiago-whatever/ProyectoFinal_NoSQL/tree/main/Queries) folder of this repository.

## MongoDB
To transfer data from the API to MongoDB, a Python script was developed. This script establishes connections to both the API and MongoDB. However, it was found that requests with the URL ending in '/discover/movie' did not provide sufficient information for the project. Therefore, this request is used to obtain movie IDs, and another request is made to a different URL. To avoid unnecessarily populating the database, the decision was made to select the top 20 most popular movies for each year from 1990 to 2024.

```python
url = f"{base_url}/movie/{id}?language=en-US"
        response = requests.get(url, headers=headers)
        results = response.json()
        if results:
            insert_Mongo(movies_collection, results)

```
It was decided to have two collections: "movies" and "credits". To handle this, a different method is used to download the data corresponding to credits, and it is shown below:

```python
url = f"{base_url}/movie/{id}/credits?language=en-US"
        response = requests.get(url, headers=headers)
        results = response.json()
        if results:
            insert_Mongo(credits_collection, results)
```
Finally, it is worth highlighting the 'insert_Mongo' method, which helped populate the different collections with the various datasets:
```python
def insert_Mongo(collection, data):
    collection.insert_one(data)
```

For the movie request, which populates the "movies" collection, the response looked like the following:
```json
{"_id":{"$oid":"656848b139c28fac6b06808b"},
"adult":false,"backdrop_path":"/sw7mordbZxgITU877yTpZCud90M.jpg",
"belongs_to_collection":null,
"budget":25000000,
"genres":[{"id":18,"name":"Drama"},{"id":80,"name":"Crime"}],
"homepage":"http://www.warnerbros.com/goodfellas",
"id":769,"imdb_id":"tt0099685",
"original_language":"en",
"original_title":"GoodFellas",
"overview":"The true story of Henry Hill, a half-Irish, half-Sicilian Brooklyn kid who is adopted by neighbourhood gangsters at an early age and climbs the ranks of a Mafia family under the guidance of Jimmy Conway.",
"popularity":85.372,
"poster_path":"/aKuFiU82s5ISJpGZp7YkIr3kCUd.jpg",
"production_companies":[{"id":8880,"logo_path":"/fE7LBw7Jz8R29EABFGCvWNriZxN.png","name":"Winkler Films",
"origin_country":"US"}],
"production_countries":[{"iso_3166_1":"US","name":"United States of America"}],
"release_date":"1990-09-12",
"revenue":46835000,
"runtime":145,
"spoken_languages":[{"english_name":"Italian","iso_639_1":"it","name":"Italiano"},{"english_name":"English","iso_639_1":"en","name":"English"}],
"status":"Released",
"tagline":"Three decades of life in the mafia.",
"title":"GoodFellas",
"video":false,
"vote_average":8.466,
"vote_count":11921}
```

A portion of the response we obtained for credits (the entire document is too long to include, and the cast is extensive for each movie) is shown below:
```json
{
  "id": 769,
  "cast": [
    {
      "adult": false,
      "gender": 2,
      "id": 11477,
      "known_for_department": "Acting",
      "name": "Ray Liotta",
      "original_name": "Ray Liotta",
      "popularity": 33.296,
      "profile_path": "/iXKotiB0Xe9iJLCBbjAedHPLb7p.jpg",
      "cast_id": 17,
      "character": "Henry Hill",
      "credit_id": "52fe4274c3a36847f801fd1f",
      "order": 0
    }
...
```

## Cassandra
To perform the ETL process from MongoDB to Cassandra, several steps were involved, which we will see below.
In addition to establishing the connection, a find({}) operation is performed for each of the collections we have. It's worth mentioning that, beforehand, the keyspace in Cassandra is defined for operational purposes: "mov" is the keyspace used, but it is verified that there is no existing keyspace with that name. In case such a keyspace exists, it will be utilized.
```python
#Creation of keyspace
keyspace_name = "mov"

existing_keyspaces = cluster.metadata.keyspaces
if keyspace_name not in existing_keyspaces:
    session.execute(f"CREATE KEYSPACE {keyspace_name} WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};")
```

Next, the two tables are generated upon which we perform queries:
```python
#Table creation
create_movie_cast_query = """
    CREATE TABLE IF NOT EXISTS movie_cast (
        movie_id INT,
        cast_id INT,
        gender INT,
        name TEXT,
        character TEXT,
        popularity FLOAT,
        known_for_department TEXT,
        PRIMARY KEY (movie_id, cast_id, name)
    )
"""


create_movies_query = """
    CREATE TABLE IF NOT EXISTS movies (
        movie_id INT,
        title TEXT,
        release_date DATE,
        genres SET<TEXT>,
        popularity FLOAT,
        budget FLOAT,
        revenue FLOAT,
        runtime INT,
        original_language TEXT,
        production_companies SET<TEXT>,
        production_countries SET<TEXT>,
        spoken_languages SET<TEXT>,
        PRIMARY KEY (movie_id, popularity)
    )
"""
```
Finally, with the help of a cursor, the information is extracted from MongoDB, the transformation is carried out (we take the parts of the JSON documents that we want), and we load our tables into Cassandra. The following code snippet accomplishes this:

```python
#Funcion to insert data in movies table
def insert_movies(data):
    query = """
        INSERT INTO movies (movie_id, title, release_date, genres, popularity, budget, revenue, runtime, original_language, production_companies, production_countries, spoken_languages)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)
    for entry in data:
        try:
            # Establecer valores predeterminados si no existen datos en algunos campos
            production_companies = entry.get('production_companies', [])
            production_countries = entry.get('production_countries', [])
            spoken_languages = entry.get('spoken_languages', [])
            genres = entry.get('genres', [])

            session.execute(prepared, (
                entry['id'],
                entry['title'],
                entry['release_date'],
                set([genre['name'] for genre in genres]),
                entry['popularity'],
                entry['budget'],
                entry['revenue'],
                entry['runtime'],
                entry['original_language'],
                set([company['name'] for company in production_companies]),
                set([country['name'] for country in production_countries]),
                set([language['name'] for language in spoken_languages])
            ))
        except Exception as e:
            print(f"Error inserting entry: {entry}, Error: {str(e)}")
    return
```

## Neo4j
To carry out the ETL process from MongoDB to Neo4j, several steps were involved, which we will see below. First, the connection to both databases is established. For the transformation, all the information from both collections is retrieved with the help of a cursor, which will be processed using the Python pandas library.

```python
#Get data from mongo
cursor = mongo_collection.find()
df = pd.DataFrame(list(cursor))
```
The processing to turn the cursor data into a graph is done as follows:
 ```python
#title,genre, release_date df
df.drop(columns=['_id','adult', 'backdrop_path', 'belongs_to_collection', 'budget', 'homepage','status', 
                        'tagline', 'video', 'vote_average', 'vote_count','imdb_id', 'original_language', 'original_title',
                        'overview', 'popularity', 'poster_path', 'revenue'], inplace=True)
df_normalized = df.explode('genres')
df_normalized['genres'] = df_normalized['genres'].apply(lambda x: x['name'] if isinstance(x, dict) else None)
df_genero = df_normalized[['id','genres','title','release_date']]
df_genero['release_date'] = pd.to_datetime(df_genero['release_date'], format='%Y-%m-%d', errors='coerce')
```
We generate the graph at last and it is loaded to Neo4j
```python
# Load1
def load1(tx, movie_id, genre, title, release_date):
    year = release_date.year

    # Create nodes
    tx.run("""
        MERGE (g:Genre {name: $genre})
        MERGE (m:Movie {id: $id, title: $title})
        MERGE (y:Year {value: $year})
    """, genre=genre, id=movie_id, title=title, year=year)

    # Crea las relaciones ENTRE entre género, película y año
    tx.run("""
        MATCH (g:Genre {name: $genre})
        MATCH (m:Movie {id: $id})
        MATCH (y:Year {value: $year})
        MERGE (m)-[:IS_GENRE_OF]->(g)
        MERGE (m)-[:RELEASED_IN]->(y)
    """, genre=genre, id=movie_id, year=year)

# Load to neo4j
with GraphDatabase.driver(uri,auth=('neo4j', 'neoneo4j')) as driver:
    with driver.session() as session:
        for index, row in df_genero.iterrows():
```



# Disclaimer:

"This applicacion uses TMDB and the TMDB APIs but is not endorsed, certified, or otherwise approved by TMDB."
