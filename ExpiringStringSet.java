import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExpiringStringSet {
    private final Map<String, Long> cache;
    private final int ttl;

    public ExpiringStringSet(int ttl) {
        this.ttl = ttl;

        this.cache = Collections.synchronizedMap(new LinkedHashMap<String, Long>(32767, 0.75F, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                if (System.currentTimeMillis() - eldest.getValue() > ttl) {
                    System.out.printf("* Key '%s' removed from the set.\n", eldest.getKey());
                    return true;
                }

                return false;
            }
        });
    }

    public void set(String key) {
        Long val = cache.putIfAbsent(key, System.currentTimeMillis());
        if (val == null) {
            System.out.printf("* Key '%s' added to the set.\n", key);
        }
    }

    public void unset(String key) {
        Long val = cache.remove(key);
        if (val != null) {
            System.out.printf("* Key '%s' removed from the set.\n", key);
        }
    }

    public boolean contains(String key) {
        final long time = System.currentTimeMillis();

        Long value = cache.get(key);
        if (value == null) {
            return false;
        }

        if (time - value > ttl) {
            unset(key);
            return false;
        }

        return true;
    }
}
