package com.fishsun666.model;

import lombok.*;
import org.apache.calcite.linq4j.function.NonDeterministic;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @Author: zhangxinsen
 * @Date: 2023/5/7 22:35
 * @Desc:
 * @Version: v1.0
 */

@AllArgsConstructor
@NonDeterministic
@Builder
@Data
@Getter
@Setter
public class SiteShouldSign {
    private String waybillNo;
    private String destSendCenterCode;
    private Date sendTime;
    private Date lastSignTime;
    private String alone;
    private String transferSiteCode;
    private String signSiteCode;
    private String timeLimitCode;
    private Date signTime;
    private String singDelay;
    private String problemNum;
    private String effectiveProblemNum;
    private String invalidProblemNum;
    private String sendSiteCode;
    private String sendFirstSiteCode;
    private String sendCenterCode;
    private Date sendDate;
    private String destSiteCode;
    private String destFirstSiteCode;
    private String destCenterCode;
    private String destProvinceName;
    private String totalPiece;
    private String specialNum;
    private String goodsName;
    private String packageType;
    private String productLineCode;
    private String productLineName;
    private String productTypeCode;
    private String productTypeName;
    private String siteFeeContent;
    private String deliveryTypeCode;
    private String goodsType;
    private String goodsCategory;
    private String dataValid;
    private Date createTime;
    private Date updateTime;
    private String nextSiteCode;
    private String scanCode;
    private String scanName;
    private BigDecimal calcWeight;
    private BigDecimal goodsVolume;
    private BigDecimal volumeWeight;
    private String advantageWaybill;
    private String paymentType;
    private String arriveFee;
    private String codFee;
    private Integer signType;
    private String abnormalSignDesc;
    private String abnormalSignCode;
    private String nextSiteTimeLimitCode;
    private Date nextSiteLastSignTime;
    private String secondSiteCode;
    private String secondSiteScanCode;
    private String secondSiteScanName;
    private String secondSiteTimeLimitCode;
    private Date secondSiteLastSignTime;
    private String deliveryCode;
    private String deliveryName;
    private Date deliveryTime;
    private String receiptWaybillNo;
    private String receiveAddress;
}
