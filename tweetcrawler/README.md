# CS685 Project


**Twitter Data**

Pull data from Twitter


Required Tools:

1. Java 1.8

2. Maven


- Prerequisite: Java 1.8

- Before Run: Go to: https://apps.twitter.com/app/new and register a new app. (e.g. cs685project)

- Once created, get the Consumer Key (API Key) and Consumer Secret (API Secret), and paste them into the twitter_dev.properties

- To Run: Place the twitter_dev.properties file and twitterdata-jar-with-dependencies.jar in a folder on the same level.

- Run the following command:

  On Mac / Linux
```java
java -cp twitterdata-jar-with-dependencies.jar:. justinchuch.twitter.crawler.PullData "hello world" 2016-09-26 2016-09-27 en
```
  On Windows
```java
java -cp twitterdata-jar-with-dependencies.jar;. justinchuch.twitter.crawler.PullData "hello world" 2016-09-26 2016-09-27 en
```

- The program arguments are:

  * [0]: keyword, e.g. "hello world"
  * [1]: since when, e.g. "2016-09-26"
  * [2]: until when, e.g. "2016-09-26"
  * [3]: language, e.g. en

  Example: "hello world" 2016-09-26 2016-09-27 en
