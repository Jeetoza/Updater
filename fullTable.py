import os
from datetime import datetime

import blpapi
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from dateutil.rrule import *

import maps

# defining variables
DATE = blpapi.Name("date")
ERROR_INFO = blpapi.Name("errorInfo")
EVENT_TIME = blpapi.Name("EVENT_TIME")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
SECURITY = blpapi.Name("security")
SECURITY_DATA = blpapi.Name("securityData")


def get_historical_ref_data(identifier, identifier_value, fields_map, startDate, endDate):
    """

    @string identifier: 'isin', 'cusip', etc.
    @string identifier_value: value corresponding to identifier
    @map fields_map:
    @string startDate: YYYYMMDD
    @string endDate: YYYYMMDD
    @return: pandas.DataFrame()

    Getting histoical data for given identifier and for multiple fields in dataframe
    """
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerAddress('localhost', 8194, 0)
    sessionOptions.setNumStartAttempts(1)
    sessionOptions.setAutoRestartOnDisconnection(True)
    session = blpapi.Session(sessionOptions)
    if not session.start():
        print("Failed to start session.")
        return 0
    identity = session.createIdentity()
    if not session.openService("//blp/refdata"):
        print("Failed to open service")
        return 0
    service = session.getService("//blp/refdata")
    request = service.createRequest("HistoricalDataRequest")
    sec = "/%s/%s" % (identifier, identifier_value)
    request.append("securities", sec)
    all_fields = []
    for fields in fields_map.values():
        if type(fields) == str:
            request.append("fields", fields)
            all_fields.append(fields)
        else:
            for field in fields:
                request.append("fields", field)
                all_fields.append(field)
    request.set("periodicitySelection", "DAILY")
    request.set("startDate", startDate)
    request.set("endDate", endDate)
    eventQueue = blpapi.EventQueue()
    session.sendRequest(request, identity, blpapi.CorrelationId("AddRequest"),
                        eventQueue)
    outDF = pd.DataFrame()
    while True:
        # Specify timeout to give a chance for Ctrl-C
        event = eventQueue.nextEvent(500)

        if event.eventType() == blpapi.Event.TIMEOUT:
            continue

        for msg in event:
            output = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA)
            security = output.getElement(SECURITY).getValueAsString()
            fieldDataArray = output.getElement(FIELD_DATA)
            fieldDataList = [fieldDataArray.getValueAsElement(i) for i in range(0, fieldDataArray.numValues())]
            dates = map(lambda x: x.getElement(DATE).getValueAsString(), fieldDataList)
            outDF = pd.DataFrame(index=dates, columns=all_fields)
            outDF.index = pd.to_datetime(outDF.index)

            for field in all_fields:
                data = []
                for row in fieldDataList:
                    if row.hasElement(field):
                        data.append(row.getElement(field).getValueAsFloat())
                    else:
                        data.append(pd.np.nan)

                outDF[field] = data

        if event.eventType() == blpapi.Event.RESPONSE:
            break
    session.stop()
    if len(outDF):
        return outDF
    else:
        print("No Data found")


def get_ref_data(identifier, identifier_value, fields_map):
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerAddress('localhost', 8194, 0)
    sessionOptions.setNumStartAttempts(1)
    sessionOptions.setAutoRestartOnDisconnection(True)
    session = blpapi.Session(sessionOptions)
    if not session.start():
        print("Failed to start session.")
        return 0
    identity = session.createIdentity()
    session.openService("//blp/refdata")
    service = session.getService("//blp/refdata")
    request = service.createRequest("ReferenceDataRequest")
    sec = "/%s/%s" % (identifier, identifier_value)
    request.append("securities", sec)
    all_fields = []
    for fields in fields_map.values():
        if type(fields) == str:
            request.append("fields", fields)
            all_fields.append(fields)
        else:
            for field in fields:
                request.append("fields", field)
                all_fields.append(field)
    eventQueue = blpapi.EventQueue()
    session.sendRequest(request, identity, blpapi.CorrelationId("AddRequest"),
                        eventQueue)
    data = pd.DataFrame()
    while True:
        # Specify timeout to give a chance for Ctrl-C
        event = eventQueue.nextEvent(500)

        if event.eventType() == blpapi.Event.TIMEOUT:
            continue

        for msg in event:
            securityData = msg.getElement(SECURITY_DATA)
            securityDataList = [securityData.getValueAsElement(i) for i in range(securityData.numValues())]
            data = pd.DataFrame(index=[0], columns=all_fields)

            for sec in securityDataList:
                fieldData = sec.getElement(FIELD_DATA)
                fieldDataList = [fieldData.getElement(i) for i in range(fieldData.numElements())]

                for fld in fieldDataList:
                    data[str(fld.name())] = fld.getValue()
        if event.eventType() == blpapi.Event.RESPONSE:
            break
    session.stop()
    if len(data):
        return data
    else:
        print("No Data found")


def get_rating_ref_data(identifier, identifier_value, fields_map, dates):
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerAddress('localhost', 8194, 0)
    sessionOptions.setNumStartAttempts(1)
    sessionOptions.setAutoRestartOnDisconnection(True)
    session = blpapi.Session(sessionOptions)
    if not session.start():
        print("Failed to start session.")
        return 0
    session.openService("//blp/refdata")
    data = pd.DataFrame()

    for date in dates:
        refdate = pd.to_datetime(date).strftime("%Y%m%d")
        service = session.getService("//blp/refdata")
        request = service.createRequest("ReferenceDataRequest")
        sec = "/%s/%s" % (identifier, identifier_value)
        request.append("securities", sec)
        all_fields = []
        for fields in fields_map.values():
            if type(fields) == str:
                request.append("fields", fields)
                all_fields.append(fields)
            else:
                for field in fields:
                    request.append("fields", field)
                    all_fields.append(field)
        identity = session.createIdentity()
        override_elements = [
            ["RATING_AS_OF_DATE_OVERRIDE", refdate]
        ]
        overrides = request.getElement("overrides")
        for elem in override_elements:
            override = overrides.appendElement()
            override.setElement("fieldId", elem[0])
            override.setElement("value", elem[1])

        eventQueue = blpapi.EventQueue()
        session.sendRequest(request, identity, blpapi.CorrelationId("AddRequest"),
                            eventQueue)
        while True:
            # Specify timeout to give a chance for Ctrl-C
            event = eventQueue.nextEvent(500)

            if event.eventType() == blpapi.Event.TIMEOUT:
                continue

            for msg in event:
                securityData = msg.getElement(SECURITY_DATA)
                securityDataList = [securityData.getValueAsElement(i) for i in range(securityData.numValues())]

                for sec in securityDataList:
                    fieldData = sec.getElement(FIELD_DATA)
                    fieldDataList = [fieldData.getElement(i) for i in range(fieldData.numElements())]

                    for fld in fieldDataList:
                        data.loc[date, str(fld.name())] = fld.getValue()
            if event.eventType() == blpapi.Event.RESPONSE:
                break
    session.stop()
    if len(data):
        return (data)
    else:
        print("No Data found")


def get_calc_ref_data(identifier, identifier_value, fields_map, df):
    data = df.copy()
    for rid, row in df.iterrows():

        date = pd.to_datetime(rid)
        settlement_date = list(rrule(DAILY, count=3, byweekday=(MO, TU, WE, TH, FR), dtstart=date))[2]
        refdate = date.strftime("%Y%m%d")
        settlment_dt = settlement_date.strftime("%Y%m%d")
        for key, field in fields_map.items():
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerAddress('localhost', 8194, 0)
            sessionOptions.setNumStartAttempts(1)
            sessionOptions.setAutoRestartOnDisconnection(True)
            session = blpapi.Session(sessionOptions)
            if not session.start():
                print("Failed to start session.")
                return 0
            session.openService("//blp/refdata")
            service = session.getService("//blp/refdata")
            sec = "/%s/%s" % (identifier, identifier_value)
            request = service.createRequest("ReferenceDataRequest")
            request.append("securities", sec)
            all_fields = []
            request.append("fields", field)
            identity = session.createIdentity()
            if field == 'INT_ACC':
                override_elements = [
                    ["USER_LOCAL_TRADE_DATE", refdate]
                ]
            elif key == "ASW$":
                override_elements = [
                    ["SETTLE_DT", settlment_dt],
                    ["OAS_CURVE_DT", refdate],
                    ["ASW_SWAP_CURRENCY", "USD"],
                    ["ASW_SWAP_PAY_RESET_FREQ", "SA"],
                    ["YAS_BOND_PX", row.PX_LAST]
                ]
            elif key == "ASW FX":
                override_elements = [
                    ["SETTLE_DT", settlment_dt],
                    ["OAS_CURVE_DT", refdate],
                    ["YAS_BOND_PX", row.PX_LAST],
                    ["ASW_SWAP_PAY_RESET_FREQ", 4]
                ]
            elif field == "DUR_MID":
                override_elements = [
                    ["PX_BID", row.PX_BID],
                    ["PX_ASK", row.PX_ASK],
                    ["YAS_BOND_PX", row.PX_LAST],
                    ["YLD_YTM_ASK", row.YLD_YTM_ASK],
                    ["YLD_YTM_BID", row.YLD_YTM_BID],
                    ["USER_LOCAL_TRADE_DATE", refdate],
                    ["SETTLE_DT", settlement_date]
                ]
            else:
                override_elements = []
            overrides = request.getElement("overrides")
            for elem in override_elements:
                override = overrides.appendElement()
                override.setElement("fieldId", elem[0])
                override.setElement("value", elem[1])

            eventQueue = blpapi.EventQueue()
            session.sendRequest(request, identity, blpapi.CorrelationId("AddRequest"),
                                eventQueue)
            while True:
                # Specify timeout to give a chance for Ctrl-C
                event = eventQueue.nextEvent(500)

                if event.eventType() == blpapi.Event.TIMEOUT:
                    continue

                for msg in event:
                    securityData = msg.getElement(SECURITY_DATA)
                    securityDataList = [securityData.getValueAsElement(i) for i in range(securityData.numValues())]

                    for sec in securityDataList:
                        fieldData = sec.getElement(FIELD_DATA)
                        fieldDataList = [fieldData.getElement(i) for i in range(fieldData.numElements())]

                        if len(fieldDataList):
                            for fld in fieldDataList:
                                col = "ASW$" if key == "ASW$" else str(fld.name())
                                data.loc[date, col] = fld.getValue()
                        else:
                            col = "ASW$" if key == "ASW$" else field
                            data.loc[date, col] = np.nan
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            session.stop()
    if len(data):
        return (data)
    else:
        print("No Data found")


def final_table(isin, start_date, end_date):
    print('running final table')
    print("getting ref data not historical")
    ref_df = get_ref_data('isin', isin, maps.const_flds_map)
    first_settle_date = ref_df.FIRST_SETTLE_DT[0] + relativedelta(days=3)
    if first_settle_date > datetime.strptime(start_date, "%Y%m%d").date():
        start_date = first_settle_date.strftime("%Y%m%d")
    print('getting historical ref data')
    historical_df = get_historical_ref_data('isin', isin, maps.histotical_flds_map, start_date, end_date)
    if len(historical_df) > 1:
        ref_df = ref_df.append([ref_df] * (len(historical_df) - 1), ignore_index=True)
    ref_df = ref_df.set_index(historical_df.index)
    print('getting rating data')
    rating_df = get_rating_ref_data('isin', isin, maps.ratings_map, historical_df.index.values)
    print('Getting FX data')
    ccy = ref_df['CRNCY'][0]
    FX_df = get_historical_ref_data('bbgid', '%s Curncy' % ccy, {'FX': "PX_LAST"}, start_date, end_date)
    FX_df.columns = ['FX']
    combine_df = pd.concat([ref_df, historical_df, rating_df, FX_df], axis=1, join="inner")
    print('Getting calculated columns')
    calc_col_df = get_calc_ref_data('isin', isin, maps.calc_map, combine_df)
    result_df = calc_col_df.rename(columns=dict(zip(maps.fld_map.values(), maps.fld_map.keys())))
    result_df["DIRTY"] = result_df['IA'] + result_df['LAST']
    result_df = result_df.fillna('')
    for col, arr in maps.new_col_map.items():
        result_df[col] = result_df.apply(lambda row: row[arr[1]] if row[arr[0]] == "" else row[arr[0]], axis=1)
    print(result_df.head())
    if ccy in ['EUR', 'GBP', 'AUD', 'NZD']:
        result_df['USD BN'] = result_df["AMOUNT OUTSTANDING"] * result_df['FX'] / 1e9
    else:
        result_df['USD BN'] = result_df["AMOUNT OUTSTANDING"] / (result_df['FX'] * 1e9)
    result_df['ISIN'] = isin
    return result_df


def update_data(country, end_date):
    """
    @datetime end_date:

    takes end date as input and updates data till end_date from last updated data in files
    TO DO: removing initial data, retail only needed history
    To DO: when adding extra field, get historical data of added field and don't pull any extra data
    """
    base_path = 'C:\Jeet\Data'
    sec_filename = '\securities\%s curves all bonds.csv' % country
    end_date_str = end_date.strftime("%Y%m%d")  # end_date in string
    secs = pd.read_csv(base_path + sec_filename)  # getting list of all securities to update
    isins = secs.ISIN

    # getting dataframe corresponding to particular country
    for isin in isins:
        print(isin)
        file_name = "C:\Jeet\Data\Database\%s.csv" % (isin)
        # Checking if file corresponding identifier exists or not
        if os.path.exists(file_name):
            # if file exists, only pull new data, starting from last date of existing data
            sec_df = pd.read_csv(file_name, index_col=0, parse_dates=True)
            prev_max_date = max(sec_df.index)
            if prev_max_date.date() == end_date.date():
                continue
            sec_df = sec_df[sec_df.index < prev_max_date]
            prev_max_date_str = prev_max_date.strftime("%Y%m%d")
            new_sec_df = final_table(isin, prev_max_date_str, end_date_str)
            new_sec_df = pd.concat([sec_df, new_sec_df])
            new_sec_df.to_csv(file_name, index_label='Date')
        else:
            # If file doesn't exist, get all the data
            start_date = end_date + relativedelta(years=-1)
            print(start_date)
            start_date_str = start_date.strftime("%Y%m%d")  # start_date in string
            sec_df = final_table(isin, start_date_str, end_date_str)
            sec_df.to_csv(file_name, index_label='Date')


if __name__ == '__main__':
    update_data('Ukraine', datetime.today())
    # df = final_table('XS1303918939 Corp', "20191020", "20191023")
    # print(df)
