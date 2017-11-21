/**
 * Tweet Crawler
 *
 * https://github.com/justinchuch
 *
 * @author justinchuch
 *
 */
package justinchuch.twitter.crawler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.ConfigurationBuilder;

public class PullData {

  private static final Logger logger = LogManager.getLogger("twitter.crawler");

  private static String CONSUMER_KEY = null;
  private static String CONSUMER_SECRET = null;
  private static int CALLS_PER_RUN = 1; // default 1
  private static boolean ISDEBUG = false;
  private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  static {
    try {
      ResourceBundle rb = ResourceBundle.getBundle("twitter_dev");
      CONSUMER_KEY = rb.getString("CONSUMER_KEY");
      CONSUMER_SECRET = rb.getString("CONSUMER_SECRET");
      String callsPerRun = rb.getString("CALLS_PER_RUN");
      try {
        CALLS_PER_RUN = Integer.parseInt(callsPerRun.trim());
      } catch (Exception e) {
        // cannot parse CALLS_PER_RUN
      }
      ISDEBUG = "true".equals(rb.getString("DEBUG"));
    } catch (Exception e) {
      // cannot parse twitter_dev
    }
  }


  public static void main(String[] args) {
    if (CONSUMER_KEY != null && CONSUMER_SECRET != null) {

      String keyword = null;
      String since = null;
      String until = null;
      String lang = null;
      try {
        keyword = args[0];
        since = args[1];
        until = args[2];
        lang = args[3];
      } catch (Exception e) {
        // missing any one of the 4 variables
      }

      if (keyword == null || since == null || until == null || lang == null) {
        logger.info("Incorrect input arguments!");
        logger.info("args[0] : keyword");
        logger.info("args[1] : since");
        logger.info("args[2] : until");
        logger.info("args[3] : lang");
        logger.info("Sample Usage:");
        logger.info("\"hello world\" 2016-09-01 2016-09-02 en");
        System.exit(-1);
      }


      // generate run log filename
      StringBuilder sb = new StringBuilder();
      sb.append(genOutputFilePrefix(keyword, since, until, lang));
      sb.append("_run.log");
      String runLogFilename = sb.toString();


      // get the maxId from runLog if any
      long maxId = -1;
      try {
        maxId = readRunLog(runLogFilename);
      } catch (Exception e) {
        logger.error(e);
      }
      if (maxId == -1) {
        logger.info("maxId not found in " + runLogFilename);
      } else if (maxId == 0) {
        logger.info("maxId = 0 , no more records!");
      } else {
        logger.info("maxId : " + maxId + " found in " + runLogFilename);
      }


      if (maxId != 0) {
        int sysStatus = 0;

        long runts = System.currentTimeMillis();

        // start
        try {
          long nextMaxId = start(keyword, since, until, lang, maxId, runts);
          logger.debug("nextMaxId=" + nextMaxId);

          outputRunLog(runLogFilename,
              genLogContent(runts, maxId, keyword, since, until, lang, nextMaxId));

          sysStatus = 0;
        } catch (Exception e) {
          sysStatus = -1;
          logger.error(e);
        }

        System.exit(sysStatus);
      }

    } else {
      logger.info("CONSUMER_KEY not found!");
      logger.info("CONSUMER_SECRET not found!");
      logger.info("Please check twitter_dev.properties file!");
      System.exit(-1);
    }

  }


  /**
   *
   * @return
   * @throws TwitterException
   * @throws IllegalStateException
   */
  private static OAuth2Token getOAuth2Token() throws TwitterException, IllegalStateException {
    OAuth2Token token = null;
    ConfigurationBuilder cb;

    cb = new ConfigurationBuilder();
    cb.setApplicationOnlyAuthEnabled(true);

    cb.setOAuthConsumerKey(CONSUMER_KEY).setOAuthConsumerSecret(CONSUMER_SECRET);

    try {
      token = new TwitterFactory(cb.build()).getInstance().getOAuth2Token();
    } catch (TwitterException e) {
      logger.info("Could not get OAuth2 token [1]");
      throw e;
    } catch (IllegalStateException e) {
      logger.info("Could not get OAuth2 token [2]");
      throw e;
    }

    return token;
  }


  /**
   *
   * @param queryKeyword
   * @param since
   * @param until
   * @param lang
   * @param maxId
   * @param runts
   * @return
   * @throws TwitterException
   * @throws IllegalStateException
   * @throws JSONException
   * @throws IOException
   */
  private static long start(String queryKeyword, String since, String until, String lang,
      long maxId, long runts)
      throws TwitterException, IllegalStateException, JSONException, IOException {
    OAuth2Token token = getOAuth2Token();

    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setJSONStoreEnabled(true); // use in json

    cb.setApplicationOnlyAuthEnabled(true);
    cb.setOAuthConsumerKey(CONSUMER_KEY);
    cb.setOAuthConsumerSecret(CONSUMER_SECRET);
    cb.setOAuth2TokenType(token.getTokenType());
    cb.setOAuth2AccessToken(token.getAccessToken());

    // Twitter twitter = TwitterFactory.getSingleton();
    Twitter twitter = new TwitterFactory(cb.build()).getInstance();

    Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("search");
    RateLimitStatus searchTweetsRateLimit = rateLimitStatus.get("/search/tweets");

    logger.info("You have %d calls remaining out of %d, Limit resets in %d seconds\n",
        searchTweetsRateLimit.getRemaining(), searchTweetsRateLimit.getLimit(),
        searchTweetsRateLimit.getSecondsUntilReset());

    long prevMaxId = maxId;

    String outfilePrefix = genOutputFilePrefix(queryKeyword, since, until, lang);
    // store the outputs into a folder
    File outputDir = new File(outfilePrefix);
    if (!outputDir.exists() && !outputDir.isDirectory()) {
      outputDir.mkdirs();
    }

    // make 5 calls
    for (int j = 0; j < CALLS_PER_RUN; j++) {
      // start from maxId
      Query q = createQuery(queryKeyword, since, until, Query.ResultType.recent, lang, maxId);
      QueryResult r = twitter.search(q); // Make the call

      long[] fromToTid = calcNextMaxId2(r, maxId);
      maxId = fromToTid[0];
      logger.debug("first tid=" + fromToTid[1] + " , last tid=" + fromToTid[2]);

      if (prevMaxId == maxId) {
        // can be ended, no more records
        logger.info("No more records");
        break;
      } else {
        // create output filename
        StringBuilder sb = new StringBuilder();
        sb.append(outfilePrefix);
        sb.append("_");
        sb.append(fromToTid[2]); // toId is smaller than fromId
        sb.append("-");
        sb.append(fromToTid[1]);
        sb.append(".json");
        String outputFilename = sb.toString();

        // output to file
        outputResult(r, outputDir, outputFilename);
      }

    } // END, for j

    return maxId;
  } // END, start


  /**
   *
   * @param keyword
   * @param since
   * @param until
   * @param resultType
   * @param lang
   * @param maxId
   * @return
   */
  private static Query createQuery(String keyword, String since, String until,
      Query.ResultType resultType, String lang, long maxId) {
    logger.debug("createQuery : maxId=" + maxId);

    Query q = new Query(keyword);
    q.setCount(100); // max 100
    q.setSince(since);
    q.setUntil(until);
    q.setResultType(resultType);
    q.setLang(lang);
    if (maxId != -1) {
      q.setMaxId(maxId - 1);
    }
    return q;
  }


  /**
   *
   * @param r
   * @param maxId
   * @return [0]: nextMaxId , [1]: fromId , [2]: toId
   * @throws JSONException
   */
  private static long[] calcNextMaxId2(QueryResult r, long maxId) throws JSONException {
    long[] fromToTid = {maxId, maxId, maxId};

    long toTid = maxId;
    int i = 0;
    Status first = r.getTweets() != null && !r.getTweets().isEmpty() ? r.getTweets().get(0) : null;
    if (first != null) {
      fromToTid[1] = first.getId();
    }
    for (Status s : r.getTweets()) {
      long tid = s.getId();

      toTid = s.getId();

      if (maxId == -1 || maxId > tid) {
        maxId = tid;
      }

      String statusJson = TwitterObjectFactory.getRawJSON(s);

      // JSON String to JSONObject
      JSONObject JSON_complete = new JSONObject(statusJson);

      if (ISDEBUG) {
        String created_at = JSON_complete.get("created_at").toString();
        String text = JSON_complete.get("text").toString();
        text = cleanText(text);
        logger
            .debug("[" + i + "] : tid=" + tid + " , created_at=" + created_at + " , text=" + text);
      }

      i++;
    } // END, for status

    // next maxId
    fromToTid[0] = maxId;

    // last id
    fromToTid[2] = toTid;

    return fromToTid;
  } // END, calcNextMaxId2


  /**
   *
   * @param r
   * @param outputDir
   * @param fileName
   * @throws IOException
   */
  private static void outputResult(QueryResult r, File outputDir, String fileName)
      throws IOException {
    int size = r.getTweets() != null ? r.getTweets().size() : 0;

    if (size > 0) {

      try (FileOutputStream fos = new FileOutputStream(new File(outputDir, fileName));
          OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
          BufferedWriter bw = new BufferedWriter(osw);) {

        bw.append("[");

        if (size > 0) {
          Status s = r.getTweets().get(0);
          String statusJson = TwitterObjectFactory.getRawJSON(s);
          bw.append(statusJson).append("\r\n");

          for (int i = 1; i < size; i++) {
            s = r.getTweets().get(i);
            statusJson = TwitterObjectFactory.getRawJSON(s);
            bw.append(",");
            bw.append(statusJson).append("\r\n");
          }
        }

        bw.append("]");

        bw.flush();
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        logger.error(e);
      }

    } // END, if size>0

  } // END, outputResult


  /**
   *
   * @param logFilename
   * @return
   */
  private static long readRunLog(String logFilename) {
    long maxId = -1;
    File logF = new File(logFilename);
    try {
      if (logF.exists() && logF.isFile()) {
        try (BufferedReader br =
            new BufferedReader(new InputStreamReader(new FileInputStream(logFilename), "UTF-8"));) {
          String cLine;
          while ((cLine = br.readLine()) != null) {
            String[] strArr = cLine.split(",");
            // nextMaxId is at the last element
            String lastEle = strArr[strArr.length - 1];
            String[] strArr2 = lastEle.split(":");
            String nextMaxIdStr = strArr2[1];

            long nextMaxId = Long.parseLong(nextMaxIdStr);
            if (maxId == -1 || nextMaxId < maxId) {
              maxId = nextMaxId;
            }

          } // END, while
        } catch (Exception e) {
          throw e;
        }
      }
    } catch (Exception e) {
      logger.error(e);
    }
    return maxId;
  }


  /**
   *
   * @param runts
   * @param prevMaxId
   * @param keyword
   * @param since
   * @param until
   * @param lang
   * @param nextMaxId
   * @return
   */
  private static String genLogContent(long runts, long prevMaxId, String keyword, String since,
      String until, String lang, long nextMaxId) {
    StringBuilder logContent = new StringBuilder();
    logContent.append("runts:");
    logContent.append(runts);
    logContent.append(",rundate:");
    logContent.append("\"");
    logContent.append(SDF.format(new java.util.Date(runts)));
    logContent.append("\"");
    logContent.append(",prevMaxId:");
    logContent.append(prevMaxId);
    logContent.append(",keyword:");
    logContent.append("\"");
    logContent.append(keyword);
    logContent.append("\"");
    logContent.append(",since:");
    logContent.append(since);
    logContent.append(",until:");
    logContent.append(until);
    logContent.append(",lang:");
    logContent.append(lang);
    logContent.append(",nextMaxId:");
    logContent.append((prevMaxId != nextMaxId ? nextMaxId : 0));
    return logContent.toString();
  }


  /**
   *
   * @param logFilename
   * @param logContent
   * @throws IOException
   */
  private static void outputRunLog(String logFilename, String logContent) throws IOException {
    // append existing file if any
    try (BufferedWriter bw = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(logFilename, true), "UTF-8"));) {
      bw.append(logContent).append("\r\n");
      bw.flush();
    } catch (IOException ioe) {
      throw ioe;
    }
  }


  /**
   *
   * @param queryKeyword
   * @param since
   * @param until
   * @param lang
   * @return
   */
  private static String genOutputFilePrefix(String queryKeyword, String since, String until,
      String lang) {
    StringBuilder sb = new StringBuilder();
    sb.append(queryKeyword.replaceAll(" ", "_"));
    sb.append("_");
    sb.append(since);
    sb.append("_");
    sb.append(until);
    sb.append("_");
    sb.append(lang);
    return sb.toString();
  }


  /**
   *
   * @param text
   * @return
   */
  private static String cleanText(String text) {
    text = text.replace("\n", "\\n");
    text = text.replace("\t", "\\t");
    return text;
  }

}
