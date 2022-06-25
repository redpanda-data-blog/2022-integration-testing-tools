import org.jsmart.zerocode.core.domain.Scenario;
import org.jsmart.zerocode.core.domain.TargetEnv;
import org.jsmart.zerocode.core.runner.ZeroCodeUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(ZeroCodeUnitRunner.class)
@TargetEnv("redpanda.properties")
public class RedpandaTest {
    @Test
    @Scenario("redpanda-stream-test.json")
    public void test_redpanda() {
    }

}
