/**
 * @author justinchuch
 *
 */
package justinchuch.twitter.crawler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

public class PullDataTest {

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Test
  public void testMain() {
    exit.expectSystemExitWithStatus(-1);
//    PullData.main(new String[] { "hello world", "2016-09-01", "2016-09-02", "en" });
    PullData.main(new String[] {});
  }

}
