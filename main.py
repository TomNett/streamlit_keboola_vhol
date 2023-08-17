from datetime import datetime
import os
import streamlit as st
import st_connection.keboola.keboola_connection
import pandas as pd
import streamlit_highcharts as hct
import keboola_api as kb

# Set logo image path and put in on to the right top
logo_image = "/data/in/files/img.png"
logo_html = f'<div style="display: flex; justify-content: flex-end;"><img src="data:image/png;base64,{base64.b64encode(open(logo_image, "rb").read()).decode()}" style="width: 100px; margin-left: -10px;"></div>'
st.markdown(f"{logo_html}", unsafe_allow_html=True)

# Read the CSV file
file_path = "/data/in/tables/full.csv"
df_data = pd.read_csv(file_path)
       
st.markdown('''
<style>
.stButton > button:focus{
    box-shadow:unset;
}
.main .block-container{
    max-width: unset;
    padding-left: 9em;
    padding-right: 9em;
    padding-top: 1.5em;
    padding-bottom: 1em;
    }
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
st.markdown("## RFM Segmentation")

  

st.markdown("## Simulate Discount on Segments") 

segTarget=st.multiselect("Segment Target:",segment, default=["Champions","Hibernating customers","Loyal","Need Attention"])
if len(segTarget)>0: 
    c,c2=st.columns(2)
    discount=c.slider("Discount on Target Segment:",min_value=0,max_value=50,step=5,value=5)
    increase=c2.slider("Anticipated Sales Increase on Target Segment:",min_value=0,max_value=50,value=20)
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
    co.metric("Revenue Current","{:,.0f}€".format(cur).replace(',', ' '))
    co1.metric("Revenue Impact","{:,.0f}€".format(sim).replace(',', ' '),str(round(((sim/cur)-1)*100,2)) + "%")
    chartdef2={
        "chart": {
                "type": 'column',
                "zoomType": 'x'
            },
            "xAxis": {
                "type": 'category'
            },
            "yAxis":{
                "title":""
            },
            "title": {
                "text": ''
            },
            "series": [
                    {   "type": 'column',
                        "dataSorting": {
                            "enabled": True,
                            "matchByName": True
                        },
                    "name":"Actual Revenue",
                    "data": parsed
                    },
                    {  "type": 'column',
                        "dataSorting": {
                            "enabled": True,
                            "matchByName": True
                        },
                    "name":"Simulated Revenue",
                    "data": parsedDisc,
                    "color":"red"
                }
            ]
            
    }
    hct.streamlit_highcharts(chartdef2)
    # with st.expander("Trigger Marketing Campaign"):
    st.markdown("## Trigger Marketing Campaign") 
    seg=",".join("'{0}'".format(w) for w in segTarget)
    query=f'''SELECT RFM.CUSTOMER_ID, RFM.SEGMENT, CUST.CUSTOMER_EMAIL, '{discount}%' as DISCOUNT
        FROM "bdm_rfm" as RFM
        INNER JOIN "bdm_customers" as CUST
        ON RFM.CUSTOMER_ID=CUST.CUSTOMER_ID
        WHERE RFM.actual_state=true AND RFM.SEGMENT in ({seg});
        '''
    dfCust = pd.read_sql(query, session)
    st.dataframe(dfCust,use_container_width=True)
    colB,cc=st.columns(2)    
    bck=colB.selectbox("Select Keboola Bucket:",key="bck",options= list(map(lambda v: v['id'], buckets)))
    date_time = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
    value = kb.keboola_upload(
        keboola_URL=keb_session.root_url,
        keboola_key=keb_session.token,
        keboola_table_name="Marketing_Discount_" +date_time,
        keboola_bucket_id=bck,
        keboola_file_path=saveFile(dfCust),
        keboola_primary_key=[""],
        label="UPLOAD TABLE",
        key="two"
    )
    value
#TODO
# Write list of steps for troubleshooting
# create Snowpark UDF function
# Call the udf function
# Add button to Save data back to Keboola (trough storage client)

#Publish doc in temp
