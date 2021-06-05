package MasterManagers.utils;

public enum StrategyTypeEnum {
    DISCOVER(1),

    RECOVER(2),

    INVALID(3),

    ;

    /**
     * 策略类型编码
     */
    private final Integer code;

    StrategyTypeEnum(int code){
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }
}
