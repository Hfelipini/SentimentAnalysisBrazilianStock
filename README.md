# SentimentAnalysisBrazilianStock
Data Engineer Project with sentiment analysis of Brazilian Stock Exchange (BSE) using twitter and yahoo as data source
Check what the users are talking about the Brazilian Stock Exchange in Twitter without having to access it, search user by user and quantify the sentiment

What is the main question?
  - Does the BSE moves with the twitter average sentiment?

![image](https://user-images.githubusercontent.com/22395461/179866156-f86c849b-8889-4255-9fe0-24a98cd37704.png)

Tools:
To answer this question and identify the movements we will be using the tools below:
- Docker: Postgres & Cassandra
- Tweepy - Twitter API for Python
- Google Translate API

Rules:
- Use only Brazilian Financial Influencers;
- All influencers have the same weight;
- If an influencer publishes 2 or more times per day, it will be calculated an average from all the day tweets, then it will be calculated with the other author's averages.

Data Architecture:

For this project it was created a relative simple structured table, as shown below:
![image](https://user-images.githubusercontent.com/22395461/183540916-11bf1952-27c3-4450-9017-206b772f78a2.png)

How does it work?
1st - To begin, it is necessary do run the docker-compose.yml to start the Postgres and Cassandra Databases;
2nd - Open the file "Python - Sentiment Analysis - V3.py" and fill the Twitter API's environment variables;
3rd - Run the code and wait until the code run is completed;
4th - Open the file "DB Analysis - SA - V2.py"
5th - Run it with the docker on, the following picture will be shown:
![Result_Polarity_Return](https://user-images.githubusercontent.com/22395461/183541914-313cccfd-9f00-428d-8acb-5f325eb5d793.jpg)

The correlation getting the last 120 days from 08/08/2022 was 0.23269 between the twitter average sentiment and the BSE close value, which means, there is a slight correlation.
For further studies, it would be healthy to get a higher sample and also give weights for every Twitter Author by the number of followers.