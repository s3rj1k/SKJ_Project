class Operation {
    public static final String operationSetLocalValue = "set-local-value";
    public static final String operationSetValue = "set-value";
    public static final String operationGetLocalValue = "get-local-value";
    public static final String operationGetValue = "get-value";
    public static final String operationFindLocalKey = "find-local-key";
    public static final String operationFindKey = "find-key";
    public static final String operationGetLocalMax = "get-local-max";
    public static final String operationGetMax = "get-max";
    public static final String operationGetLocalMin = "get-local-min";
    public static final String operationGetMin = "get-min";
    public static final String operationNewRecord = "new-record";
    public static final String operationUnregister = "unregister";
    public static final String operationTerminate = "terminate";
    public static final String operationQuit = "quit";
    public static final String operationPing = "ping";
    public static final String operationRegister = "register";
    public static final String operationListRemotes = "list-remotes";

    public static final String resultOk = "OK";
    public static final String resultError = "ERROR";

    public String operation;
    public String parameter;
    public Integer key;
    public Integer value;

    public void parse(String data) throws ApplicationException {
        data = data.trim();
        try {
            if (data.startsWith(operationSetLocalValue + " ")) {
                String param = data.substring(operationSetLocalValue.length()).trim();
                this.operation = operationSetLocalValue;
                this.parameter = param;
                this.key = Record.parseKey(param);
                this.value = Record.parseValue(param);
            } else if (data.startsWith(operationSetValue + " ")) {
                String param = data.substring(operationSetValue.length()).trim();
                this.operation = operationSetValue;
                this.parameter = param;
                this.key = Record.parseKey(param);
                this.value = Record.parseValue(param);
            } else if (data.startsWith(operationGetLocalValue + " ")) {
                String param = data.substring(operationGetLocalValue.length()).trim();
                this.operation = operationGetLocalValue;
                this.parameter = param;
                this.key = Integer.parseInt(param);
            } else if (data.startsWith(operationGetValue + " ")) {
                String param = data.substring(operationGetValue.length()).trim();
                this.operation = operationGetValue;
                this.parameter = param;
                this.key = Integer.parseInt(param);
            } else if (data.startsWith(operationFindLocalKey + " ")) {
                String param = data.substring(operationFindLocalKey.length()).trim();
                this.operation = operationFindLocalKey;
                this.parameter = param;
                this.key = Integer.parseInt(param);
            } else if (data.startsWith(operationFindKey + " ")) {
                String param = data.substring(operationFindKey.length()).trim();
                this.operation = operationFindKey;
                this.parameter = param;
                this.key = Integer.parseInt(param);
            } else if (data.equals(operationGetLocalMax)) {
                this.operation = operationGetLocalMax;
            } else if (data.equals(operationGetMax)) {
                this.operation = operationGetMax;
            } else if (data.equals(operationGetLocalMin)) {
                this.operation = operationGetLocalMin;
            } else if (data.equals(operationGetMin)) {
                this.operation = operationGetMin;
            } else if (data.startsWith(operationNewRecord + " ")) {
                String param = data.substring(operationNewRecord.length()).trim();
                this.operation = operationNewRecord;
                this.parameter = param;
                this.key = Record.parseKey(param);
                this.value = Record.parseValue(param);
            } else if (data.startsWith(operationUnregister + " ")) {
                String param = data.substring(operationUnregister.length()).trim();
                this.operation = operationUnregister;
                this.key = Integer.parseInt(param);
            } else if (data.equals(operationTerminate)) {
                this.operation = operationTerminate;
            } else if (data.equals(operationQuit)) {
                this.operation = operationQuit;
            } else if (data.equals(operationPing)) {
                this.operation = operationPing;
            } else if (data.startsWith(operationRegister + " ")) {
                String param = data.substring(operationRegister.length()).trim();
                this.operation = operationRegister;
                this.parameter = param;
                this.key = Integer.parseInt(param);
            } else if (data.equals(operationListRemotes)) {
                this.operation = operationListRemotes;
            } else {
                throw new ApplicationException(String.format("Unknown operation requested -> `%s`.", data));
            }
        } catch (IllegalArgumentException e) {
            throw new ApplicationException(String.format("Invalid value in operation parameters -> `%s`.", data));
        }
    }
}
