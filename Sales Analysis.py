import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go


username = '' #Please enter username here
password = '' #Please enter password here


engine = create_engine('mysql+pymysql://{}:{}@localhost/random_sales_data_db'.format(username, password),
                       connect_args= dict(host='localhost', port=3306))
conn = engine.connect()

query = '''
        SELECT territory.territory_name States,
               ROUND(SUM(transactions.sales_price_$)) Sales
        FROM ((transactions 
        INNER JOIN sales_person ON transactions.sales_person_id = sales_person.sales_person_id)
        INNER JOIN territory ON territory.territory_id = sales_person.territory_id)
        GROUP BY States
        ORDER BY Sales DESC;
        '''

df = pd.read_sql(query, conn)
df = df.iloc[[0, -1]]


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


fig = go.Figure([go.Bar(x=df['States'], y=df['Sales'], width=0.4)])
fig.update_layout(
            height=500,
            plot_bgcolor='#ECE4E4',
            margin={
                'pad': 0,
                't': 0,
                'r': 0,
                'l': 0,
                'b': 0,
            },
        )
fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False)


app.layout = html.Div(children=[
    html.H1(children='Regional Sales Analysis'),
    html.H3(children='Best and Worst Performing States'),
    html.Div(children=[
        dcc.Graph(
            id='example-graph',
            figure=fig,
        
    )], style={
            'width': '75%',
            'height': '75vh',
            'display': 'inline-block',
            'overflow': 'hidden',
            'position': 'absolute',
            'top': '60%',
            'left': '50%',
            'transform': 'translate(-50%, -50%)'
})
], style={
    'text-align':'center'
})

if __name__ == '__main__':
    app.run_server(debug=True)
