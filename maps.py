fld_map = {
    "SECURITY NAME" : "SECURITY_NAME",
    "NAME" : "OBLIGOR_NAME",
    "MATURITY" : "MATURITY",
    "CRNCY" : "CRNCY",
    "DUAL" : "DUAL_CRNCY",
    "CPN" : "CPN_TYP",
    "LAST" : "PX_LAST",
    "DUR" : "DUR_MID",
    "RANK" : "PAYMENT_RANK",
    "TYPE" : "MTY_TYP",
    "INDUSTRY" : "INDUSTRY_SUBGROUP",
    "SERIES" : "SERIES",
    "CALC" : "CALC_TYP_DES",
    "IA" : "INT_ACC",
    "AMOUNT OUTSTANDING" : "AMT_OUTSTANDING",
    "CTRY" : "CNTRY_OF_RISK",
    "MDY LC" : "RTG_MDY_LC_CURR_ISSUER_RATING",
    "MDY FC" : "RTG_MDY_FC_CURR_ISSUER_RATING",
    "MOODY FC" : "RTG_MOODY",
    "SP FC" : "RTG_SP_LT_FC_ISSUER_CREDIT",
    "FITCH FC" : "RTG_FITCH_LT_ISSUER_DEFAULT",
    "1 SETTLE" : "FIRST_SETTLE_DT"
}
new_col_map = {
    "YTM" : ["YLD_YTM_BID", "YLD_YTM_MID"],
    "ASW FX" : ["YAS_ASW_SPREAD", "ASSET_SWAP_SPD_MID"],
}
histotical_flds_map = {
    "PRICE" : "PX_LAST",
    "YTM" : ["YLD_YTM_BID", "YLD_YTM_MID", "YLD_YTM_ASK"],
    "ASW FX" : "ASSET_SWAP_SPD_MID",
    "extra" : ["PX_BID", "PX_ASK" ]
}
const_flds_map = {
    "SECURITY NAME" : "SECURITY_NAME",
    "NAME" : "OBLIGOR_NAME",
    "MATURITY" : "MATURITY",
    "CRNCY" : "CRNCY",
    "DUAL" : "DUAL_CRNCY",
    "CPN" : "CPN_TYP",
    "RANK" : "PAYMENT_RANK",
    "TYPE" : "MTY_TYP",
    "INDUSTRY" : "INDUSTRY_SUBGROUP",
    "SERIES" : "SERIES",
    "CALC" : "CALC_TYP_DES",
    "AMOUNT_OUTSTANDING" : "AMT_OUTSTANDING",
    "CTRY" : "CNTRY_OF_RISK",
    "1 SETTLE" : "FIRST_SETTLE_DT"
}
ratings_map = {
    "MDY LC" : "RTG_MDY_LC_CURR_ISSUER_RATING",
    "MDY FC" : "RTG_MDY_FC_CURR_ISSUER_RATING",
    "MOODY FC" : "RTG_MOODY",
    "SP FC" : "RTG_SP_LT_FC_ISSUER_CREDIT",
    "FITCH FC" : "RTG_FITCH_LT_ISSUER_DEFAULT",
}
calc_map = {
    "ASW FX" : "YAS_ASW_SPREAD",
    "ASW$" : "YAS_ASW_SPREAD",
    "IA" : "INT_ACC",
    "DUR" : "DUR_MID"
}