class KV {
    private final char delimiter = Operation.parameterDelimiter;

    public Integer key;
    public Integer value;

    public KV(Integer key, Integer value) {
        this.key = key;
        this.value = value;
    }

    public KV(String key, String value) throws ApplicationException {
        try {
            this.key = Integer.parseInt(key);
            this.value = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ApplicationException(String.format("Invalid KV input (not Integer): %s", e.getMessage()));
        }
    }

    public KV(Integer key) {
        this.key = key;
        this.value = null;
    }

    public KV(String kv) throws ApplicationException {
        int delimiterIndex = kv.indexOf(this.delimiter);
        if (delimiterIndex == 0) {
            throw new ApplicationException("Invalid KV input (unexpected length)");
        } else if (delimiterIndex < 0) {
            try {
                this.key = Integer.parseInt(kv);
            } catch (NumberFormatException e) {
                throw new ApplicationException(String.format("Invalid KV input (not Integer): %s", e.getMessage()));
            }

            this.value = null;

            return;
        }

        try {
            this.key = Integer.parseInt(kv.substring(0, delimiterIndex));
        } catch (NumberFormatException e) {
            throw new ApplicationException(String.format("Invalid KV input (not Integer): %s", e.getMessage()));
        } catch (IndexOutOfBoundsException e) {
            throw new ApplicationException(String.format("Invalid KV input (unexpected length): %s", e.getMessage()));
        }

        if ((delimiterIndex + 1) > kv.length()) {
            throw new ApplicationException("Invalid KV input (unexpected length)");
        }

        try {
            this.value = Integer.parseInt(kv.substring(delimiterIndex + 1));
        } catch (NumberFormatException e) {
            throw new ApplicationException(String.format("Invalid KV input (not Integer): %s", e.getMessage()));
        } catch (IndexOutOfBoundsException e) {
            throw new ApplicationException(String.format("Invalid KV input (unexpected length): %s", e.getMessage()));
        }
    }

    public KV() {
        this.key = null;
        this.value = null;
    }

    @Override
    public String toString() {
        if (this.key == null) {
            return null;
        }

        if (this.value == null) {
            return this.key.toString();
        }

        return String.format("%d%s%d", this.key, this.delimiter, this.value);
    }
}
