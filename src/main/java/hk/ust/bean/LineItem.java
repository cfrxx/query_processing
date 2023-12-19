package hk.ust.bean;

import java.time.LocalDate;

public class LineItem {
    private String flag;
    private Integer l_orderKey;
    private Integer l_partKey;
    private Integer l_suppKey;
    private Integer l_lineNumber;
    private Double l_quantity;
    private Double l_extendedPrice;
    private Double l_discount;
    private Double l_tax;
    private String l_returnFlag;
    private String l_lineStatus;
    private LocalDate l_shipDate;
    private LocalDate l_commitDate;
    private LocalDate l_receiptDate;
    private String l_shipInstruct;
    private String l_shipMode;
    private String l_comment;

    public LineItem() {
    }

    public LineItem(String flag, Integer l_orderKey, Integer l_partKey, Integer l_suppKey, Integer l_lineNumber, Double l_quantity, Double l_extendedPrice, Double l_discount, Double l_tax, String l_returnFlag, String l_lineStatus, LocalDate l_shipDate, LocalDate l_commitDate, LocalDate l_receiptDate, String l_shipInstruct, String l_shipMode, String l_comment) {
        this.flag = flag;
        this.l_orderKey = l_orderKey;
        this.l_partKey = l_partKey;
        this.l_suppKey = l_suppKey;
        this.l_lineNumber = l_lineNumber;
        this.l_quantity = l_quantity;
        this.l_extendedPrice = l_extendedPrice;
        this.l_discount = l_discount;
        this.l_tax = l_tax;
        this.l_returnFlag = l_returnFlag;
        this.l_lineStatus = l_lineStatus;
        this.l_shipDate = l_shipDate;
        this.l_commitDate = l_commitDate;
        this.l_receiptDate = l_receiptDate;
        this.l_shipInstruct = l_shipInstruct;
        this.l_shipMode = l_shipMode;
        this.l_comment = l_comment;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Integer getL_orderKey() {
        return l_orderKey;
    }

    public void setL_orderKey(Integer l_orderKey) {
        this.l_orderKey = l_orderKey;
    }

    public Integer getL_partKey() {
        return l_partKey;
    }

    public void setL_partKey(Integer l_partKey) {
        this.l_partKey = l_partKey;
    }

    public Integer getL_suppKey() {
        return l_suppKey;
    }

    public void setL_suppKey(Integer l_suppKey) {
        this.l_suppKey = l_suppKey;
    }

    public Integer getL_lineNumber() {
        return l_lineNumber;
    }

    public void setL_lineNumber(Integer l_lineNumber) {
        this.l_lineNumber = l_lineNumber;
    }

    public Double getL_quantity() {
        return l_quantity;
    }

    public void setL_quantity(Double l_quantity) {
        this.l_quantity = l_quantity;
    }

    public Double getL_extendedPrice() {
        return l_extendedPrice;
    }

    public void setL_extendedPrice(Double l_extendedPrice) {
        this.l_extendedPrice = l_extendedPrice;
    }

    public Double getL_discount() {
        return l_discount;
    }

    public void setL_discount(Double l_discount) {
        this.l_discount = l_discount;
    }

    public Double getL_tax() {
        return l_tax;
    }

    public void setL_tax(Double l_tax) {
        this.l_tax = l_tax;
    }

    public String getL_returnFlag() {
        return l_returnFlag;
    }

    public void setL_returnFlag(String l_returnFlag) {
        this.l_returnFlag = l_returnFlag;
    }

    public String getL_lineStatus() {
        return l_lineStatus;
    }

    public void setL_lineStatus(String l_lineStatus) {
        this.l_lineStatus = l_lineStatus;
    }

    public LocalDate getL_shipDate() {
        return l_shipDate;
    }

    public void setL_shipDate(LocalDate l_shipDate) {
        this.l_shipDate = l_shipDate;
    }

    public LocalDate getL_commitDate() {
        return l_commitDate;
    }

    public void setL_commitDate(LocalDate l_commitDate) {
        this.l_commitDate = l_commitDate;
    }

    public LocalDate getL_receiptDate() {
        return l_receiptDate;
    }

    public void setL_receiptDate(LocalDate l_receiptDate) {
        this.l_receiptDate = l_receiptDate;
    }

    public String getL_shipInstruct() {
        return l_shipInstruct;
    }

    public void setL_shipInstruct(String l_shipInstruct) {
        this.l_shipInstruct = l_shipInstruct;
    }

    public String getL_shipMode() {
        return l_shipMode;
    }

    public void setL_shipMode(String l_shipMode) {
        this.l_shipMode = l_shipMode;
    }

    public String getL_comment() {
        return l_comment;
    }

    public void setL_comment(String l_comment) {
        this.l_comment = l_comment;
    }
}
