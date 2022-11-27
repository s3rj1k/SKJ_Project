class Record {
    public static Integer getSplitterIndex(String record) {
        for (int i = (record.length() - 1); i > 0; i--) {
            if (String.valueOf(record.charAt(i)).equals(":")) {
                return i;
            }
        }

        return (record.length() - 1);
    }

    public static Integer parseKey(String record) throws NumberFormatException {
        String prefix = record.substring(0, getSplitterIndex(record));
        return Integer.parseInt(prefix);
    }

    public static Integer parseValue(String record) throws NumberFormatException {
        String suffix = record.substring(getSplitterIndex(record) + 1);
        return Integer.parseInt(suffix);
    }
}
