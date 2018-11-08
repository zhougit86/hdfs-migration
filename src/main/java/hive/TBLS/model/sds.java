package hive.TBLS.model;

public class sds {
    private Long sdId;

    private Long cdId;

    private String inputFormat;

    private Boolean isCompressed;

    private Boolean isStoredassubdirectories;

    private String location;

    private Integer numBuckets;

    private String outputFormat;

    private Long serdeId;

    public Long getSdId() {
        return sdId;
    }

    public void setSdId(Long sdId) {
        this.sdId = sdId;
    }

    public Long getCdId() {
        return cdId;
    }

    public void setCdId(Long cdId) {
        this.cdId = cdId;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat == null ? null : inputFormat.trim();
    }

    public Boolean getIsCompressed() {
        return isCompressed;
    }

    public void setIsCompressed(Boolean isCompressed) {
        this.isCompressed = isCompressed;
    }

    public Boolean getIsStoredassubdirectories() {
        return isStoredassubdirectories;
    }

    public void setIsStoredassubdirectories(Boolean isStoredassubdirectories) {
        this.isStoredassubdirectories = isStoredassubdirectories;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location == null ? null : location.trim();
    }

    public Integer getNumBuckets() {
        return numBuckets;
    }

    public void setNumBuckets(Integer numBuckets) {
        this.numBuckets = numBuckets;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat == null ? null : outputFormat.trim();
    }

    public Long getSerdeId() {
        return serdeId;
    }

    public void setSerdeId(Long serdeId) {
        this.serdeId = serdeId;
    }
}