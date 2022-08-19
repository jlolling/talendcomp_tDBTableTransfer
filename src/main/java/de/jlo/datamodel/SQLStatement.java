package de.jlo.datamodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Eintrag in der Historie
 * Klasse kapselt ausgeführte SQL-Statements mit Zusatzinformationen
 */
public class SQLStatement implements Serializable {

	private static final long serialVersionUID = 1L;
	static final public int    OTHER                      = 0;
    static final public int    QUERY                      = 1;
    static final public int    UPDATE                     = 2;
    static final public int    START                      = 3;
    static final public int    EXPLAIN                    = 4;
    static int                 lastIndex                  = -1;
    private int                index;
    protected String           sql;
    protected String           sql_temp;                                 // das eigentliche SQL-Statement
    private long               executedAt                 = 0;           // wann ausgeführt
    private long               durationExec               = 0;
    private long               durationGet                = 0;
    private boolean            successful                 = false;       // erfolgreich ausgeführt ?
    private int                type;
    private int                startPos;
    private int                endPos;
    private String             message                    = "";
    protected boolean          hidden                     = false;
    public static final String START_SEQUENCE_FOR_SUMMARY = "-[";
    public static final String SQL_PARAM_DELIMITER        = "\u0000";
    public static final String PARAM_PARAM_DELIMITER      = "\u0001";
    private List<SQLPSParam>   paramList                  = new Vector<SQLPSParam>();
    private boolean            isPrepared                 = false;
    private boolean            hasNamedParams             = false;
    private boolean            sqlCodeValid               = true; // falls bei der Erstellung erkannt wird, dass der SQLCode fehlerhaft sein muss !
    private String             currentFile                = null;
    private boolean isExecuting = false;
    private boolean isGettingData = false;
    private String currentUrl;
    private String currentUser;
    public static String ignoreResultSetComment = "/*ignore_resultset*/";
    
    public SQLStatement(String sql) {
        this.sql = sql.replace('\r', ' ');
        type = checkType();
        index = ++lastIndex;
    }

    public SQLStatement() {}

    public String getSQL() {
        return sql;
    }

    public void setSQL(String sql_loc) {
        this.sql = sql_loc.replace('\r', ' ');
        type = checkType();
    }

    public void setStartTime() {
        isExecuting = true;
        this.executedAt = System.currentTimeMillis();
    }

    public void setExecStopTime() {
        isExecuting = false;
        if (type == QUERY) {
            isGettingData = true;
        }
        durationExec = System.currentTimeMillis() - executedAt;
    }

    public void setGetStopTime() {
        isGettingData = false;
        durationGet = System.currentTimeMillis() - executedAt + durationExec;
    }

    public long getDurationExec() {
        return durationExec;
    }

    public long getDurationGet() {
        return durationGet;
    }

    public java.util.Date getExecutionDate() {
        return new java.util.Date(executedAt);
    }

    /**
     * Status der Ausführung
     * @return true wenn Statement ausgeführt wurde, false wenn noch nicht gestartet
     */
    public boolean isStarted() {
        if (executedAt == 0) {
            return false;
        } else {
            return true;
        }
    }
    
    public boolean isRunning() {
        return isExecuting || isGettingData;
    }

    public void setTextRange(int startPos_loc, int endPos_loc) {
        this.startPos = startPos_loc;
        this.endPos = endPos_loc;
    }

    public int getStartPos() {
        return startPos;
    }

    public int getEndPos() {
        return endPos;
    }

    public int getIndex() {
        return index;
    }

    static public void resetIndex() {
        lastIndex = -1;
    }

    public void setSuccessful(boolean successful_loc) {
        isGettingData = false;
        isExecuting = false;
        this.successful = successful_loc;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public void setMessage(String message_loc) {
        this.message = message_loc;
    }

    public String getMessage() {
        return message;
    }

    public String getSummary() {
        String startTime;
        if (executedAt > 0) {
            startTime = (getExecutionDate()).toString();
        } else {
            startTime = "nicht ausgeführt";
        }
        String textDurationExec = null;
        if (getDurationExec() > 1000) {
            textDurationExec = getDurationExec()
                    + "ms = "
                    + (getDurationExec() / 1000f)
                    + "s = "
                    + ((getDurationExec() / 1000f) / 60f)
                    + "min";
        } else {
            textDurationExec = getDurationExec() + "ms = " + (getDurationExec() / 1000f) + "s";
        }
        String textDurationGet = null;
        if (getDurationExec() > 1000) {
            textDurationGet = getDurationGet()
                    + "ms = "
                    + (getDurationGet() / 1000f)
                    + "s = "
                    + ((getDurationGet() / 1000f) / 60f)
                    + "min";
        } else {
            textDurationGet = getDurationGet() + "ms = " + (getDurationGet() / 1000f) + "s";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Succesfull = ");
        sb.append(successful);
        sb.append("\nStart time = ");
        sb.append(startTime);
        sb.append("\nDuration exec = ");
        sb.append(textDurationExec);
        sb.append("\nDuration fetch = ");
        sb.append(textDurationGet);
        sb.append("\n");
        sb.append(message);
        sb.append("\n");
        sb.append(getParameterSummary());
        return sb.toString();
    }
    
    private String getParameterSummary() {
        StringBuffer text = new StringBuffer(64);
        if (paramList.size() > 0) {
            SQLPSParam param = null;
            text.append("used parameters:");
            for (int i = 0; i < paramList.size(); i++) {
                param = (SQLPSParam) paramList.get(i);
                text.append('\n');
                if (param.isOutParam() == false) {
                    text.append("in ");
                } else {
                    text.append("out ");
                }
                text.append("index=");
                text.append(param.getIndex());
                if (param.getName() != null) {
                    text.append(" (");
                    text.append(param.getName());
                    text.append(") ");
                }
                text.append(" value=");
                text.append(param.getValue());
            }
        } else {
        	text.append("none prepared parameters used");
        }
        return text.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SQLStatement) {
            final SQLStatement sqlStat = ((SQLStatement) o);
            if ((((getSQL()).trim()).toLowerCase()).equals(((sqlStat.getSQL()).trim()).toLowerCase())) {
                return true;
            } else {
                return false;
            }
        } else {
        	return false;
        }
    }

    public int getType() {
        return type;
    }

    private int checkType() {
        int sqlType;
        sql_temp = sql.trim().toLowerCase();
        if (sql_temp.startsWith("insert") || sql_temp.startsWith("update") || sql_temp.startsWith("delete") || sql_temp.startsWith("merge")) {
            sqlType = UPDATE;
        } else if (sql_temp.contains(ignoreResultSetComment)) {
            sqlType = EXPLAIN;
        } else if (sql_temp.startsWith("select")) {
            sqlType = QUERY;
        } else if (sql_temp.startsWith("start") || sql_temp.startsWith("@")) {
            sqlType = START;
        } else {
            sqlType = OTHER;
        }
        return sqlType;
    }

    public void setHidden(boolean hidden_loc) {
        this.hidden = hidden_loc;
    }

    public boolean isHidden() {
        return hidden;
    }

    public String getSummaryStr() {
        return START_SEQUENCE_FOR_SUMMARY
                + String.valueOf(index)
                + "|"
                + String.valueOf(successful)
                + "|"
                + String.valueOf(executedAt)
                + "|"
                + String.valueOf(durationExec)
                + "|"
                + String.valueOf(durationGet)
                + "]";
    }

    public void parseSummaryStr(String param) {
        // index
        final int i1 = param.indexOf('|');
        index = Integer.parseInt(param.substring(0, i1));
        // flag successful
        final int i2 = param.indexOf('|', i1 + 2);
        if (param.substring(i1 + 1, i2).trim().equals("true")) {
            successful = true;
        } else {
            successful = false;
        }
        // executedAt
        final int i3 = param.indexOf('|', i2 + 1);
        executedAt = Long.parseLong(param.substring(i2 + 1, i3));
        // durationExec
        final int i4 = param.indexOf('|', i3 + 1);
        durationExec = Long.parseLong(param.substring(i3 + 1, i4));
        // durationGet
        final int i5 = param.indexOf('|', i4 + 1);
        if (i5 != -1) {
            durationGet = Long.parseLong(param.substring(i4 + 1, i5));
            currentFile = param.substring(i5 + 1, param.length());
        } else {
            durationGet = Long.parseLong(param.substring(i4 + 1, param.length()));
        }
    }

    public void addParam(SQLPSParam param) {
        paramList.add(param);
    }

    public SQLPSParam getParam(String name) {
        SQLPSParam psParam = null;
        for (int i = 0; i < paramList.size(); i++) {
            psParam = (SQLPSParam) paramList.get(i);
            if (psParam.getName().equalsIgnoreCase(name)) {
                break;
            } else {
                psParam = null;
            }
        }
        return psParam;
    }

    public SQLPSParam getParam(int paramIndex) {
        SQLPSParam psParam = null;
        for (int i = 0; i < paramList.size(); i++) {
            psParam = paramList.get(i);
            if (psParam.getIndex() == paramIndex) {
                break;
            } else {
                psParam = null;
            }
        }
        return psParam;
    }

    public boolean hasOutputParams() {
        for (SQLPSParam psParam : paramList) {
            if (psParam.isOutParam()) {
            	return true;
            }
        }
        return false;
    }
    
    public boolean hasNamedParams() {
        for (SQLPSParam psParam : paramList) {
            if (psParam.isNamedParam()) {
            	return true;
            }
        }
        return false;
    }

    public List<SQLPSParam> getOutputParams() {
        final ArrayList<SQLPSParam> list = new ArrayList<SQLPSParam>();
        for (SQLPSParam psParam : paramList) {
            if (psParam.isOutParam()) {
                list.add(psParam);
            }
        }
        return list;
    }

    /**
     * gibt alle Parameter zurück
     * @return Vector mit Instanzen der Klassen SQLPSParam
     */
    public List<SQLPSParam> getParams() {
        return paramList;
    }
    
    public int getCountParameters() {
    	return paramList.size();
    }

    public void setParams(List<SQLPSParam> params) {
        this.paramList = params;
    }

    /**
     * prüft ob das komplette Statement gültig ist,
     * bedeutet, dass alle Variablen (Parameter) gebunden sind
     * @return true wenn gültig
     */
    public boolean isValid() {
        SQLPSParam param = null;
        boolean isValid = true;
        if (sql.length() < 3) {
            isValid = false;
        }
        for (int i = 0; i < paramList.size(); i++) {
            param = paramList.get(i);
            if (!param.isValid()) {
                isValid = false;
                break;
            }
        }
        return isValid;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer(String.valueOf(index) + ": " + getSQL());
        for (int i = 0; i < paramList.size(); i++) {
            if (i == 0) {
                sb.append(" [");
            }
            sb.append('\n');
            sb.append(paramList.get(i).toString());
            if (i == paramList.size() - 1) {
                sb.append("]");
            } else {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    public String getPSParamStr() {
        final StringBuffer sb = new StringBuffer();
        for (int i = 0; i < paramList.size(); i++) {
            sb.append(PARAM_PARAM_DELIMITER);
            sb.append(paramList.get(i).toString());
        }
        return sb.toString();
    }

    public boolean isPrepared() {
        return isPrepared;
    }

    public void setPrepared(boolean isPrepared_loc) {
        this.isPrepared = isPrepared_loc;
    }

    @Override
    public int hashCode() {
        return sql.hashCode();
    }

    public boolean isSqlCodeValid() {
        return sqlCodeValid;
    }
    
    public void setSqlCodeValid(boolean sqlCodeWrong) {
        this.sqlCodeValid = sqlCodeWrong;
    }

	public boolean isHasNamedParams() {
		return hasNamedParams;
	}

	public void setHasNamedParams(boolean hasNamedParams) {
		this.hasNamedParams = hasNamedParams;
	}

	public void setFile(String file) {
		this.currentFile = file;
	}

	public String getFile() {
		return currentFile;
	}

	public boolean isStartStatement() {
		return type == START;
	}

	public String getCurrentUrl() {
		return currentUrl;
	}

	public void setCurrentUrl(String currentUrl) {
		this.currentUrl = currentUrl;
	}

	public String getCurrentUser() {
		return currentUser;
	}

	public void setCurrentUser(String currentUser) {
		this.currentUser = currentUser;
	}
	
	public void setIsExplainStatement() {
		type = EXPLAIN;
	}
    
	public boolean isExplainStatement() {
		return type == EXPLAIN;
	}
}
