import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class DB {
    Map<Integer, Integer> db = new ConcurrentHashMap<>();

    Boolean existsLocally(Integer key) {
        try {
            return this.db.containsKey(key);
        } catch (Exception e) {
            System.err.printf("Failed to check local key existence `%d`: %s\n", key, e.getMessage());
            return false;
        }
    }

    Boolean newLocalValue(Integer key, Integer value) {
        try {
            this.db.put(key, value);
            return true;
        } catch (Exception e) {
            System.err.printf("Failed to create new local value `%d:%d`: %s\n", key, value, e.getMessage());
            return false;
        }
    }

    Boolean setLocalValue(Integer key, Integer value) {
        try {
            if (this.db.containsKey(key)) {
                this.db.put(key, value);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            System.err.printf("Failed to set local value `%d:%d`: %s\n", key, value, e.getMessage());
            return false;
        }
    }

    Integer getLocalValue(Integer key) {
        try {
            return this.db.getOrDefault(key, null);
        } catch (Exception e) {
            System.err.printf("Failed to get local value `%d`: %s\n", key, e.getMessage());
            return null;
        }
    }

    KV getLocalMax() {
        KV kv = new KV();

        Integer maxKey = null;
        Integer maxValue = null;

        for (Integer key : this.db.keySet()) {
            Integer value = this.db.get(key);
            if ((maxValue == null) || (maxValue < value)) {
                maxValue = value;
                maxKey = key;
            }
        }

        kv.set(maxKey, maxValue);
        return kv;
    }

    KV getLocalMin() {
        KV kv = new KV();

        Integer minKey = null;
        Integer minValue = null;

        for (Integer key : this.db.keySet()) {
            Integer value = this.db.get(key);
            if ((minValue == null) || (minValue > value)) {
                minValue = value;
                minKey = key;
            }
        }

        kv.set(minKey, minValue);
        return kv;
    }

    public static class KV {
        public Integer key;
        public Integer value;

        public void set(Integer key, Integer value) {
            this.key = key;
            this.value = value;
        }
    }
}
