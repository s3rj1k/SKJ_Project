class Operation {
    public static final String operationSetValue = "set-value";
    public static final String operationGetValue = "get-value";
    public static final String operationFindKey = "find-key";
    public static final String operationGetMax = "get-max";
    public static final String operationGetMin = "get-min";
    public static final String operationNewRecord = "new-record";
    public static final String operationTerminate = "terminate";

    public static final String operationPing = "ping";
    public static final String operationRegister = "register";
    public static final String operationUnregister = "unregister";
    public static final String operationListRemotes = "list-remotes";

    public static final String[] knownOperations = {
            operationFindKey,
            operationGetMax,
            operationGetMin,
            operationGetValue,
            operationNewRecord,
            operationSetValue,
            operationTerminate,
            operationListRemotes,
            operationPing,
            operationRegister,
            operationUnregister,
    };

    public static final String[] knownRecursiveOperations = {
            operationFindKey,
            operationGetMax,
            operationGetMin,
            operationGetValue,
            operationSetValue,
    };

    public static final String resultOk = "OK";
    public static final String resultError = "ERROR";
    public static final String resultSeen = "SEEN";
    public static final String resultEmpty = "EMPTY";

    public static final String[] resultNotOk = {
            resultEmpty,
            resultError,
            resultSeen,
    };

    public static final char parameterDelimiter = ':';
    public static final char beginIDWrap = '[';
    public static final char endIDWrap = ']';

    public String id;
    public String operation;
    public String parameter;
    public Integer key;
    public Integer value;

    public Operation(String value) throws ApplicationException {
        try {
            this.id = null;
            this.operation = null;
            this.parameter = null;
            this.key = null;
            this.value = null;

            String data = value.trim();

            if (data.charAt(0) == beginIDWrap) {
                for (int i = 1; i < data.length(); i++) {
                    if (data.charAt(i) == endIDWrap) {
                        this.id = data.substring(1, i).trim();

                        if ((i + 1) > data.length()) {
                            throw new ApplicationException(String.format("Unknown operation (ID parse) -> `%s`.", value));
                        }

                        data = data.substring(i + 1).trim();
                        break;
                    }
                }
            }

            for (int i = 0; i < knownOperations.length; i++) {
                if (data.startsWith(knownOperations[i])) {
                    this.operation = knownOperations[i];

                    try {
                        this.parameter = data.substring(knownOperations[i].length() + 1).trim();
                    } catch (Exception e) {
                        this.parameter = null;
                    }

                    break;
                }
            }

            if (this.operation == null) {
                throw new ApplicationException(String.format("Unknown operation -> `%s`.", value));
            }

            if ((this.parameter == null) || (this.parameter.length() == 0)) {
                return;
            }

            int delimiterIndex = this.parameter.indexOf(parameterDelimiter);

            try {
                if (delimiterIndex > 0) {
                    this.key = Integer.parseInt(this.parameter.substring(0, delimiterIndex));

                    if ((delimiterIndex + 1) > this.parameter.length()) {
                        throw new ApplicationException(String.format("Unknown operation (parameter parse) -> `%s`.", value));
                    }

                    this.value = Integer.parseInt(this.parameter.substring(delimiterIndex + 1));
                } else if (delimiterIndex < 0) {
                    this.key = Integer.parseInt(this.parameter);
                }
            } catch (NumberFormatException e) {
                throw new ApplicationException(String.format("Invalid value in operation parameters -> `%s`.", value));
            }
        } catch (IndexOutOfBoundsException e) {
            throw new ApplicationException(String.format("Internal unhandled error, IndexOutOfBounds for -> `%s`.", value));
        }
    }

    @Override
    public String toString() {
        if (this.operation == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();

        if (this.id != null) {
            sb.append(beginIDWrap);
            sb.append(this.id);
            sb.append(endIDWrap);
            sb.append(" ");
        }

        sb.append(this.operation);

        if (this.key != null) {
            sb.append(" ");
            sb.append(this.key);

            if (this.value != null) {
                sb.append(parameterDelimiter);
                sb.append(this.value);
            }
        }

        return sb.toString();
    }
}
