import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class DB {
    Map<Integer, Integer> db = new ConcurrentHashMap<>();

    Boolean exists(Integer key) {
        try {
            return this.db.containsKey(key);
        } catch (Exception e) {
            System.err.printf("* Failed to check local key existence `%d`: %s\n", key, e.getMessage());
            return false;
        }
    }

    Boolean newValue(Integer key, Integer value) {
        try {
            this.db.put(key, value);
            return true;
        } catch (Exception e) {
            System.err.printf("* Failed to create new local value `%d`=`%d`: %s\n", key, value, e.getMessage());
            return false;
        }
    }

    Boolean newValue(KV kv) {
        return newValue(kv.key, kv.value);
    }

    Boolean setValue(Integer key, Integer value) {
        try {
            if (this.db.containsKey(key)) {
                this.db.put(key, value);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            System.err.printf("* Failed to set local value `%d`=`%d`: %s\n", key, value, e.getMessage());
            return false;
        }
    }

    Boolean setValue(KV kv) {
        return setValue(kv.key, kv.value);
    }

    Integer getValue(Integer key) {
        try {
            return this.db.getOrDefault(key, null);
        } catch (Exception e) {
            System.err.printf("* Failed to get local value `%d`: %s\n", key, e.getMessage());
            return null;
        }
    }

    KV getMax() {
        Integer maxKey = null;
        Integer maxValue = null;

        for (Integer key : this.db.keySet()) {
            Integer value = this.db.get(key);
            if ((maxValue == null) || (maxValue < value)) {
                maxValue = value;
                maxKey = key;
            }
        }

        return new KV(maxKey, maxValue);
    }

    KV getMin() {
        Integer minKey = null;
        Integer minValue = null;

        for (Integer key : this.db.keySet()) {
            Integer value = this.db.get(key);
            if ((minValue == null) || (minValue > value)) {
                minValue = value;
                minKey = key;
            }
        }

        return new KV(minKey, minValue);
    }
}
