"""
Interactive Dashboard with Visualizations
Built with Dash and Plotly for real-time analytics
"""

import logging
from datetime import datetime, timedelta

try:
    import dash
    from dash import dcc, html, Input, Output, callback
    import dash_bootstrap_components as dbc
    import plotly.graph_objs as go
    import plotly.express as px
    import pandas as pd
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False
    logging.warning("Dashboard dependencies not available")

from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION, MONGO_SCHEMA_COLLECTION, MONGO_CHANGES_COLLECTION
from analytics_engine import trend_analyzer, change_analyzer, recommendation_engine, data_insights


class DashboardApp:
    """Main Dashboard Application"""

    def __init__(self):
        if not DASH_AVAILABLE:
            logging.error("Dashboard not available. Install dash and plotly.")
            self.app = None
            return

        self.app = dash.Dash(
            __name__,
            external_stylesheets=[dbc.themes.BOOTSTRAP],
            suppress_callback_exceptions=True
        )

        self.db = MongoClient(MONGO_URI)[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]
        self.schema_collection = self.db[MONGO_SCHEMA_COLLECTION]
        self.changes_collection = self.db[MONGO_CHANGES_COLLECTION]

        self.setup_layout()
        self.setup_callbacks()

    def setup_layout(self):
        """Setup dashboard layout"""
        self.app.layout = dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("🤖 AI-Powered Data Pipeline Dashboard", className="text-center mb-4"),
                    html.Hr()
                ])
            ]),

            # Stats Cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Total Records", className="card-title"),
                            html.H2(id="total-records", className="text-primary")
                        ])
                    ])
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Schema Versions", className="card-title"),
                            html.H2(id="schema-versions", className="text-success")
                        ])
                    ])
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Recent Changes", className="card-title"),
                            html.H2(id="recent-changes", className="text-warning")
                        ])
                    ])
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Data Quality", className="card-title"),
                            html.H2(id="data-quality", className="text-info")
                        ])
                    ])
                ], width=3),
            ], className="mb-4"),

            # Tabs for different views
            dbc.Tabs([
                dbc.Tab(label="📊 Overview", tab_id="overview"),
                dbc.Tab(label="📈 Trends", tab_id="trends"),
                dbc.Tab(label="🔍 Change Detection", tab_id="changes"),
                dbc.Tab(label="🎯 Schema Analysis", tab_id="schema"),
                dbc.Tab(label="💡 Recommendations", tab_id="recommendations"),
            ], id="tabs", active_tab="overview"),

            html.Div(id="tab-content", className="mt-4"),

            # Auto-refresh
            dcc.Interval(id='interval-component', interval=30*1000, n_intervals=0)

        ], fluid=True)

    def setup_callbacks(self):
        """Setup dashboard callbacks"""

        @self.app.callback(
            [Output("total-records", "children"),
             Output("schema-versions", "children"),
             Output("recent-changes", "children"),
             Output("data-quality", "children")],
            Input('interval-component', 'n_intervals')
        )
        def update_stats(n):
            total_records = self.collection.count_documents({})
            schema_versions = self.schema_collection.count_documents({})

            cutoff = datetime.now() - timedelta(days=7)
            recent_changes = self.changes_collection.count_documents({
                'timestamp': {'$gte': cutoff}
            })

            # Calculate average quality score
            quality_scores = []
            for doc in self.collection.find({}, {'_data_quality_score': 1}).limit(100):
                score = doc.get('_data_quality_score')
                if score:
                    quality_scores.append(score)

            avg_quality = f"{sum(quality_scores)/len(quality_scores):.1f}%" if quality_scores else "N/A"

            return total_records, schema_versions, recent_changes, avg_quality

        @self.app.callback(
            Output("tab-content", "children"),
            Input("tabs", "active_tab"),
            Input('interval-component', 'n_intervals')
        )
        def render_tab_content(active_tab, n):
            if active_tab == "overview":
                return self.render_overview()
            elif active_tab == "trends":
                return self.render_trends()
            elif active_tab == "changes":
                return self.render_changes()
            elif active_tab == "schema":
                return self.render_schema()
            elif active_tab == "recommendations":
                return self.render_recommendations()

            return html.Div("Select a tab")

    def render_overview(self):
        """Render overview tab"""
        # Get recent data
        recent_data = list(self.collection.find({}).sort('_loaded_at', -1).limit(100))

        if not recent_data:
            return html.Div("No data available")

        df = pd.DataFrame(recent_data)

        # Remove MongoDB _id for display
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)

        # Data table
        return dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H3("Recent Data Upload Timeline"),
                    dcc.Graph(figure=self.create_upload_timeline())
                ])
            ]),
            dbc.Row([
                dbc.Col([
                    html.H3("Field Type Distribution"),
                    dcc.Graph(figure=self.create_field_type_distribution())
                ])
            ]),
        ])

    def render_trends(self):
        """Render trends analysis tab"""
        # Get latest schema
        latest_schema = self.schema_collection.find_one({}, sort=[('version', -1)])

        if not latest_schema:
            return html.Div("No schema data available")

        schema = latest_schema.get('schema', {})
        trends = trend_analyzer.get_all_trends(schema)

        if not trends:
            return html.Div("No trend data available for numeric fields")

        # Create trend charts
        charts = []
        for field, trend_data in trends.items():
            fig = self.create_trend_chart(field, trend_data)
            charts.append(dbc.Col([dcc.Graph(figure=fig)], width=6))

        return dbc.Container([
            html.H3("Field Trends Analysis"),
            dbc.Row(charts)
        ])

    def render_changes(self):
        """Render change detection tab"""
        recent_changes = change_analyzer.get_recent_changes(days=30, limit=100)

        if not recent_changes:
            return html.Div("No changes detected")

        # Convert to DataFrame
        df_changes = pd.DataFrame(recent_changes)

        # Create change timeline
        fig = self.create_change_timeline(df_changes)

        # Change patterns
        patterns = change_analyzer.analyze_change_patterns(days=30)

        return dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H3("Change Timeline"),
                    dcc.Graph(figure=fig)
                ])
            ]),
            dbc.Row([
                dbc.Col([
                    html.H3("Change Patterns"),
                    html.Pre(str(patterns))
                ])
            ])
        ])

    def render_schema(self):
        """Render schema analysis tab"""
        # Get all schema versions
        schemas = list(self.schema_collection.find({}).sort('version', -1))

        if not schemas:
            return html.Div("No schema data available")

        # Schema evolution timeline
        fig = self.create_schema_evolution_chart(schemas)

        # Current schema details
        current_schema = schemas[0]['schema']

        schema_table = []
        for field, info in current_schema.items():
            schema_table.append(html.Tr([
                html.Td(field),
                html.Td(info.get('type', 'unknown')),
                html.Td(info.get('semantic_category', 'unknown')),
                html.Td(f"{info.get('confidence', 0):.2f}"),
                html.Td(str(info.get('sample_values', [])[:2]))
            ]))

        return dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H3("Schema Evolution"),
                    dcc.Graph(figure=fig)
                ])
            ]),
            dbc.Row([
                dbc.Col([
                    html.H3("Current Schema Details"),
                    dbc.Table([
                        html.Thead(html.Tr([
                            html.Th("Field"),
                            html.Th("Type"),
                            html.Th("Semantic Category"),
                            html.Th("Confidence"),
                            html.Th("Sample Values")
                        ])),
                        html.Tbody(schema_table)
                    ], striped=True, bordered=True, hover=True)
                ])
            ])
        ])

    def render_recommendations(self):
        """Render recommendations tab"""
        latest_schema = self.schema_collection.find_one({}, sort=[('version', -1)])

        if not latest_schema:
            return html.Div("No data for recommendations")

        schema = latest_schema.get('schema', {})
        stats = latest_schema.get('stats', {})

        recommendations = recommendation_engine.generate_recommendations(schema, stats)

        if not recommendations:
            return html.Div("✅ No recommendations - everything looks good!")

        rec_cards = []
        for rec in recommendations:
            color = "danger" if rec['priority'] == 'high' else "warning" if rec['priority'] == 'medium' else "info"

            rec_cards.append(
                dbc.Alert([
                    html.H5(f"🎯 {rec['type'].replace('_', ' ').title()}", className="alert-heading"),
                    html.P(rec['message']),
                    html.Hr(),
                    html.P(f"Action: {rec['action']}", className="mb-0 font-weight-bold")
                ], color=color, className="mb-3")
            )

        return dbc.Container([
            html.H3("💡 AI-Generated Recommendations"),
            html.Div(rec_cards)
        ])

    def create_upload_timeline(self):
        """Create upload timeline chart"""
        pipeline = [
            {'$group': {
                '_id': {
                    '$dateToString': {'format': '%Y-%m-%d', 'date': '$_loaded_at'}
                },
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id': 1}}
        ]

        results = list(self.collection.aggregate(pipeline))

        if not results:
            return go.Figure()

        dates = [r['_id'] for r in results]
        counts = [r['count'] for r in results]

        fig = go.Figure(data=[
            go.Scatter(x=dates, y=counts, mode='lines+markers', name='Records Uploaded')
        ])

        fig.update_layout(
            title="Data Upload Timeline",
            xaxis_title="Date",
            yaxis_title="Records",
            hovermode='x unified'
        )

        return fig

    def create_field_type_distribution(self):
        """Create field type distribution pie chart"""
        latest_schema = self.schema_collection.find_one({}, sort=[('version', -1)])

        if not latest_schema:
            return go.Figure()

        schema = latest_schema.get('schema', {})

        type_counts = {}
        for field, info in schema.items():
            field_type = info.get('type', 'unknown')
            type_counts[field_type] = type_counts.get(field_type, 0) + 1

        fig = go.Figure(data=[
            go.Pie(labels=list(type_counts.keys()), values=list(type_counts.values()))
        ])

        fig.update_layout(title="Field Type Distribution")

        return fig

    def create_trend_chart(self, field_name, trend_data):
        """Create trend chart for a field"""
        fig = go.Figure()

        stats = trend_data.get('statistics', {})

        fig.add_trace(go.Indicator(
            mode="number+delta",
            value=stats.get('current', 0),
            title={'text': field_name},
            delta={'reference': stats.get('mean', 0), 'relative': True},
        ))

        fig.update_layout(height=250)

        return fig

    def create_change_timeline(self, df_changes):
        """Create change timeline"""
        if df_changes.empty:
            return go.Figure()

        fig = px.scatter(df_changes, x='timestamp', y='field', color='field',
                        title="Change Timeline by Field")

        fig.update_layout(showlegend=True)

        return fig

    def create_schema_evolution_chart(self, schemas):
        """Create schema evolution chart"""
        versions = [s['version'] for s in schemas]
        field_counts = [len(s['schema']) for s in schemas]

        fig = go.Figure(data=[
            go.Bar(x=versions, y=field_counts, name='Fields')
        ])

        fig.update_layout(
            title="Schema Evolution - Field Count Over Versions",
            xaxis_title="Version",
            yaxis_title="Number of Fields"
        )

        return fig

    def run(self, host='0.0.0.0', port=8050, debug=True):
        """Run dashboard server"""
        if self.app:
            self.app.run_server(host=host, port=port, debug=debug)
        else:
            logging.error("Dashboard not initialized")


# Create global dashboard instance
dashboard_app = DashboardApp()


if __name__ == '__main__':
    if DASH_AVAILABLE:
        dashboard_app.run()
    else:
        print("Dashboard dependencies not installed. Run: pip install -r requirements.txt")
