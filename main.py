import streamlit as st
import st_connection
import st_connection.snowflake
import pandas as pd
import json
import streamlit_highcharts as hct

session = st.connection.snowflake_connection.login({'user': 'KEBOOLA_WORKSPACE_26088314', 'password': '','account': 'keboola.west-europe.azure'}, { 'warehouse': 'KEBOOLA_PROD'}, 'Snowflake Login')


st.markdown('''
<style>
/*center metric label*/
[data-testid="stMetricLabel"] > div:nth-child(1) {
    justify-content: center;
}

/*center metric value*/
[data-testid="stMetricValue"] > div:nth-child(1) {
    justify-content: center;
}
[data-testid="stMetricDelta"] > div:nth-child(2){
    justify-content: center;
}

</style>
''', unsafe_allow_html=True)
st.markdown("# RFM Segmentation")

def getRevSplit(segment,discount,increase):
    ls=",".join("'{0}'".format(w) for w in segment)
    if ls=="":
        ls="''"
    queryAll=f'''
SELECT ALLSELL.TYPE,ALLSELL.product_manufacturer as PR, ROUND(sum(ALLSELL.DISC),0) AS REV
FROM
    (SELECT 'DISC' as TYPE,P.product_manufacturer, O.ORDER_LINE_PRICE_WITH_TAXES as Sales, ((Sales*{1+(increase/100)}) - Sales*{(discount/100)}) as DISC ,O.ORDER_ID, C.CUSTOMER_ID,RF.SEGMENT
        FROM "bdm_order_lines" as O 
        INNER JOIN "bdm_products" as P 
        ON P.PRODUCT_ID=O.ORDER_LINE_PRODUCT_ID
        INNER JOIN "bdm_orders" as OS
        ON O.ORDER_ID=OS.ORDER_ID
        INNER JOIN "bdm_customers" as C
        ON OS.CUSTOMER_ID=C.customer_id
        INNER JOIN "bdm_rfm" as RF
        ON RF.CUSTOMER_ID=C.customer_id WHERE RF.SEGMENT IN ({ls})
    UNION (
        (SELECT 'ALL' as TYPE,P.product_manufacturer, O.ORDER_LINE_PRICE_WITH_TAXES as Sales, Sales as DISC ,O.ORDER_ID,                      C.CUSTOMER_ID,RF.SEGMENT
        FROM "bdm_order_lines" as O 
        INNER JOIN "bdm_products" as P 
        ON P.PRODUCT_ID=O.ORDER_LINE_PRODUCT_ID
        INNER JOIN "bdm_orders" as OS
        ON O.ORDER_ID=OS.ORDER_ID
        INNER JOIN "bdm_customers" as C
        ON OS.CUSTOMER_ID=C.customer_id
        INNER JOIN "bdm_rfm" as RF
        ON RF.CUSTOMER_ID=C.customer_id WHERE RF.SEGMENT NOT IN ({ls}) )
     UNION(
        SELECT 'EXCEPT' as TYPE,P.product_manufacturer, O.ORDER_LINE_PRICE_WITH_TAXES as Sales, Sales as DISC ,O.ORDER_ID, C.CUSTOMER_ID,RF.SEGMENT
        FROM "bdm_order_lines" as O 
        INNER JOIN "bdm_products" as P 
        ON P.PRODUCT_ID=O.ORDER_LINE_PRODUCT_ID
        INNER JOIN "bdm_orders" as OS
        ON O.ORDER_ID=OS.ORDER_ID
        INNER JOIN "bdm_customers" as C
        ON OS.CUSTOMER_ID=C.customer_id
        INNER JOIN "bdm_rfm" as RF
        ON RF.CUSTOMER_ID=C.customer_id WHERE RF.SEGMENT IN ({ls})
     )   
    )) as ALLSELL
GROUP BY ALLSELL.product_manufacturer, ALLSELL.TYPE
ORDER BY REV DESC;
'''
    df = pd.read_sql(queryAll, session)
    return df

query=f'''SELECT SEGMENT, COUNT(*) as c
    FROM "bdm_rfm" 
    WHERE actual_state=true
    GROUP BY SEGMENT'''
df = pd.read_sql(query, session)
cols=st.columns(4)
cols2=st.columns(4)
allc=cols+cols2
for index, k in df.iterrows():
    with allc[index-1]:
        st.metric(k['SEGMENT'],k['C'])
query=f'''
    SELECT DISTINCT SEGMENT FROM "bdm_rfm";
'''
segment = pd.read_sql(query, session)


st.markdown("## Simulate Discount on Segments") 

segTarget=st.multiselect("Segment Target:",segment, default="Champions")  
if len(segTarget)>0:
    c,c2=st.columns(2)
    discount=c.slider("Discount on Target Segment:",min_value=0,max_value=50,step=5)
    increase=c2.slider("Anticipated Sales Increase on Target Segment:",min_value=0,max_value=50)
    df2=getRevSplit(segTarget,discount,increase)
    dfAll=df2.loc[df2['TYPE'].isin(['ALL','EXCEPT'])] 
    dfAll=dfAll.groupby('PR',as_index=False).sum()
    dfAll.sort_values(by=['PR'],inplace=True)
    result = dfAll.to_json(orient="values")
    parsed = json.loads(result)
    dfDisc=df2.loc[df2['TYPE'].isin(['ALL','DISC'])]
    dfDisc=dfDisc.groupby('PR',as_index=False).sum()
    dfDisc.sort_values('PR')
    resultDisc = dfDisc.to_json(orient="values")
    parsedDisc = json.loads(resultDisc) 
    cat=json.loads(dfDisc.PR.to_json(orient="values"))
    co,co1=st.columns(2)
    cur=dfAll.sum().REV
    sim=dfDisc.sum().REV
    co.metric("Revenue Current",cur)
    co1.metric("Revenue Impact",sim,str(round(((sim/cur)-1)*100,2)) + "%")
    chartdef2={
        "chart": {
                "type": 'column',
                "zoomType": 'x'
            },
            "xAxis":{
                "categories":cat,
                "min":0,
                "max":15,
                "scrollbar": {
                    "enabled": True
                }
            },
            "yAxis":{
                "title":""
            },
            "title": {
                "text": ''
            },
            "dataSorting":{
                "enable":True
            },
            "series": [
                {
                    "name":"Actual Revenue",
                    "data": parsed
                    },
                {
                    "name":"Simulated Revenue",
                    "data": parsedDisc,
                    "color":"red"
                }
            ]
            
    }
    hct.streamlit_highcharts(chartdef2)


#TODO
# OK Scrollbar in Highchart
# Show table with customer from segments and discount
# Keboola Write Back
# Publish App