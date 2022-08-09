""" #Pacotes para serem instalados antes de rodar o programa
#pip install requests_oauthlib --user
#pip install twython --user
#pip install nltk --user
#pip install pyspark --user
#pip install googletrans==3.1.0a0
"""

""" #Linhas de Comando para Conectar, Inserir e Ler Dados da base de dados no Postgres DB no Docker
# Postgres - Python - https://www.postgresqltutorial.com/postgresql-python/create-tables/
# Docker -> Postgres -> Terminal
# Comando Terminal: "psql -d postgres -U postgres"
# "\l" listar databases
# Comando Terminal: "use postgres"
# "\dt"
# Listas todas as tabelas
# Comando Terminal: "SELECT * FROM user_data;" -> Retorna todos os dados na tabela criada e populada
"""

#Módulos Usados
import tweepy
from textblob import TextBlob
from wordcloud import WordCloud
import pandas as pd
import re
from googletrans import Translator
import psycopg2
from cassandra.cluster import Cluster
from datetime import date
from psycopg2.errorcodes import UNIQUE_VIOLATION
from psycopg2 import errors
import os

# ------------------ Postgres Configuration -----------------------------------
conn = psycopg2.connect("host=127.0.0.1 port=7000 dbname=postgres user=postgres password=example")
print(conn)

cursor = conn.cursor()

def find_all():
  return cursor.fetchall()

#cursor.execute("DROP TABLE IF EXISTS tweet")
#cursor.execute("CREATE TABLE tweet (tweet_id CHAR(100) PRIMARY KEY, user_id CHAR(100), created_at timestamp, subjectivity int, polarity int, bet_days int)")
#conn.commit()

# ------------------ Cassandra Configuration ----------------------------------

try: 
    cluster = Cluster(['127.0.0.1'], port=6000) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
    print(session)
except Exception as e:
    print(e)

#session.execute("DROP TABLE IF EXISTS sentiment.tweet;")
#session.execute("CREATE KEYSPACE IF NOT EXISTS sentiment WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
session.execute("USE sentiment;")
#session.execute("CREATE TABLE sentiment.tweet (tweet_id text PRIMARY KEY, user_id text, created_at timestamp, tweet_original text, tweet_traduzido text, subjectivity int, polarity int);")

# -------------- Código para ler e gerar infos de Tweets no Twitter -----------

# Twitter API credentials
consumerKey = os.environ['consumerKey']
consumerSecret = os.environ['consumerSecret']
accessToken = os.environ['accessToken']
accessTokenSecret = os.environ['accessTokenSecret']

#Frequencia Update
authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)

#Set the access token and access token secret
authenticate.set_access_token(accessToken, accessTokenSecret)

# Create the API Object while passing in the auth information
api = tweepy.API(authenticate, wait_on_rate_limit = True)

#Create the translator object
translator = Translator()

count = 0

#Reads de text file with the main authors from the Brazilian financial market in Tweeter
#Extract the tweets from the twitter user
for aut in open('autores.txt'):
  count = count+1
  print(str(count) + " " + aut)

  #Get the ID from the last Tweet the author published in the Postgres.
  ID_User = api.get_user(screen_name = aut).id_str
  cursor.execute("SELECT tweet_id FROM tweet WHERE user_id = %s ORDER BY tweet_id desc",(ID_User,))
  get_all_data = cursor.fetchall()
  try:
    ultimo_id = get_all_data[0][0]
    conn.commit()
  except IndexError:
    pass
  
  #Adicionar aqui o Try Except - Unique Violation

  #Retrieve the Tweets from the last tweet ID the author posted.
  UniqueViolation = errors.lookup('23505')
  try:
    posts = api.user_timeline(screen_name = aut, since_id=ultimo_id, tweet_mode="extended")    
  except UniqueViolation as err:
    posts = api.user_timeline(screen_name =aut, count= 100, tweet_mode="extended")
  user = api.get_user(screen_name = aut)

  #Create a dataframe with columns called Tweets, Created_at, Tweet_id & User_id
  #First, we need to translate the portuguese Tweet to english to work with TextBlob.
  #This is done using the Google Translator API.
  df = pd.DataFrame( [translator.translate(tweet.full_text, src = 'pt').text for tweet in posts], columns=['Tweets'])
  df['Created_at'] = [tweet.created_at for tweet in posts]
  df['Tweet_id'] = [tweet.id for tweet in posts]
  df['User_id'] = [tweet.user.id_str for tweet in posts]

  #Create a function to clean the tweets
  def cleanTxt(text):
      text = re.sub(r'@[A-Za-z0-9]+', '', text) #Removed @mentions
      text = re.sub(r'#', '', text) #Removed the Hashtag
      text = re.sub(r'RT[\s]+', '', text) #Removed RT
      text = re.sub(r'https?:\/\/S+', '', text) #Removed the hyperlink
      return text

  # Cleaning the text
  df['Tweets'] = df['Tweets'].apply(cleanTxt)

  #Create a function to get the subjectivity
  def getSubjectivity(text):
      return TextBlob(text).sentiment.subjectivity

  #Create a function to get the polarity
  def getPolarity(text):
      return TextBlob(text).sentiment.polarity

  #Create a function to get the tweet's day to comparison
  def getDays(date1):
      return date(date1.year,date1.month,date1.day)-date(2000,1,1)

  #Create two new columns in the dataframe with the Subjectivity and Polarity
  df['Subjectivity'] = df['Tweets'].apply(getSubjectivity)*100
  df['Polarity'] = df['Tweets'].apply(getPolarity)*100
  df['Original'] = [tweet.full_text for tweet in posts]
  df['GetDays'] = df['Created_at'].apply(getDays)

  #Identify the tweets that resulted in sentiment and polarity to save them into the database for further analysis
  for linhas in range(len(df)):
    if(df.iloc[linhas][5]!=0):
        cursor.execute("INSERT INTO Tweet VALUES (%s, %s, %s, %s, %s, %s);",(int(df.iloc[linhas][2]), int(df.iloc[linhas][3]), df.iloc[linhas][1], df.iloc[linhas][4], df.iloc[linhas][5], int(df.iloc[linhas][7].days)))
        conn.commit()
    session.execute("INSERT INTO tweet (tweet_id, user_id, created_at, tweet_original, tweet_traduzido, subjectivity, polarity) VALUES (%s,%s,%s,%s,%s,%s,%s);",(str(df.iloc[linhas][2]),df.iloc[linhas][3],str(df.iloc[linhas][1]),df.iloc[linhas][6],df.iloc[linhas][0], int(df.iloc[linhas][4]), int(df.iloc[linhas][5])))
  
conn.close()