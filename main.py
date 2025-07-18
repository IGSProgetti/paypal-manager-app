import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import numpy as np
import hashlib
from github_storage import GitHubStorage

# Configurazione della pagina
st.set_page_config(
    page_title="💰 PayPal Manager - Professional",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === CONFIGURAZIONE GITHUB ===
def setup_github_storage():
    """Configura il GitHub Storage"""
    # Configurazione GitHub (da inserire nei secrets di Streamlit)
    github_token = st.secrets.get("GITHUB_TOKEN", "")
    repo_owner = st.secrets.get("GITHUB_REPO_OWNER", "")
    repo_name = st.secrets.get("GITHUB_REPO_NAME", "")
    
    if not all([github_token, repo_owner, repo_name]):
        st.error("⚠️ Configurazione GitHub mancante! Configura i secrets.")
        st.stop()
    
    return GitHubStorage(repo_owner, repo_name, github_token)

# Inizializza GitHub Storage
if 'github_storage' not in st.session_state:
    st.session_state.github_storage = setup_github_storage()

storage = st.session_state.github_storage

# === FUNZIONI HELPER ===
def clean_and_standardize_df(df):
    """Pulisce e standardizza il DataFrame PayPal"""
    # Pulisce i nomi delle colonne
    df.columns = df.columns.str.strip()
    
    # Mappa le colonne PayPal standard
    column_mapping = {
        'Data': 'data',
        'Orario': 'orario', 
        'Nome': 'nome',
        'Tipo': 'tipo_transazione',
        'Stato': 'stato',
        'Valuta': 'valuta',
        'Lordo': 'importo_lordo',
        'Netto': 'importo_netto',
        'Tariffa': 'commissione',
        'Codice transazione': 'codice_transazione',
        'Indirizzo email mittente': 'email_mittente',
        'Indirizzo email destinatario': 'email_destinatario',
        'Titolo oggetto': 'descrizione',
        'Saldo': 'saldo',
        'Impatto sul saldo': 'impatto_saldo',
        'Tipo di spesa': 'tipo_spesa',
        'Categoria': 'categoria', 
        'Tipologia': 'tipologia',
        'Sottocategoria': 'sottocategoria',
        'Numero Fattura': 'numero_fattura',
        'Note': 'note'
    }
    
    # Rinomina le colonne presenti
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
    
    # Aggiungi colonne mancanti
    required_columns = ['categoria', 'sottocategoria', 'numero_fattura', 'note', 'tipo_spesa', 'tipologia']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Pulisci e converti i dati
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
    
    if 'importo_lordo' in df.columns:
        df['importo_lordo'] = pd.to_numeric(df['importo_lordo'], errors='coerce').fillna(0)
    
    # Campi testuali
    text_fields = ['categoria', 'sottocategoria', 'numero_fattura', 'note', 'tipo_spesa', 'tipologia', 
                   'nome', 'tipo_transazione', 'stato', 'descrizione']
    
    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].fillna("").astype(str)
    
    # Codice transazione
    if 'codice_transazione' not in df.columns or df['codice_transazione'].isna().all():
        df['codice_transazione'] = 'TXN_' + df.index.astype(str)
    else:
        df['codice_transazione'] = df['codice_transazione'].fillna('').astype(str)
    
    # ID univoco
    df['id_univoco'] = df['codice_transazione'].astype(str) + '_' + df.index.astype(str)
    
    return df

@st.cache_data
def load_paypal_excel(file_path_or_uploaded_file):
    """Carica un file Excel PayPal"""
    try:
        if isinstance(file_path_or_uploaded_file, str):
            df = pd.read_excel(file_path_or_uploaded_file)
        else:
            df = pd.read_excel(file_path_or_uploaded_file)
        
        df = clean_and_standardize_df(df)
        return df
        
    except Exception as e:
        st.error(f"Errore nel caricamento del file PayPal: {str(e)}")
        return None

def load_historical_data():
    """Carica lo storico completo delle transazioni da GitHub"""
    return storage.load_dataframe("data/paypal_history_complete.csv")

def save_historical_data(df):
    """Salva lo storico completo su GitHub"""
    success = storage.save_dataframe(df, "data/paypal_history_complete.csv", "Update PayPal historical data")
    
    if success:
        st.success("✅ Dati salvati su GitHub!")
    else:
        st.error("❌ Errore nel salvataggio su GitHub")

def load_metadata():
    """Carica i metadati del sistema da GitHub"""
    return storage.load_metadata()

def save_metadata(metadata):
    """Salva i metadati del sistema su GitHub"""
    success = storage.save_metadata(metadata)
    
    if not success:
        st.error("❌ Errore nel salvataggio metadati")

def add_import_to_historical(new_data, import_metadata):
    """Aggiunge un nuovo import allo storico evitando duplicati"""
    try:
        historical_df = load_historical_data()
        
        # Prepara i nuovi dati con metadati
        new_data_copy = new_data.copy()
        new_data_copy['import_id'] = import_metadata['import_id']
        new_data_copy['import_date'] = import_metadata['import_date']
        new_data_copy['import_datetime'] = pd.to_datetime(import_metadata['import_datetime'])
        new_data_copy['import_filename'] = import_metadata['filename']
        
        if historical_df.empty:
            updated_historical = new_data_copy
        else:
            # Verifica duplicati
            if 'codice_transazione' in historical_df.columns and 'codice_transazione' in new_data_copy.columns:
                existing_codes = set(historical_df['codice_transazione'].dropna())
                new_transactions = new_data_copy[~new_data_copy['codice_transazione'].isin(existing_codes)]
                
                if len(new_transactions) == 0:
                    st.warning("⚠️ Tutte le transazioni sono già presenti nello storico")
                    return historical_df
                else:
                    st.success(f"✅ Aggiunte {len(new_transactions)} nuove transazioni (evitati {len(new_data_copy) - len(new_transactions)} duplicati)")
                    new_data_copy = new_transactions
            
            updated_historical = pd.concat([historical_df, new_data_copy], ignore_index=True)
        
        # Ordina per data
        if 'data' in updated_historical.columns:
            updated_historical = updated_historical.sort_values('data', ascending=False)
        
        save_historical_data(updated_historical)
        return updated_historical
        
    except Exception as e:
        st.error(f"Errore nell'aggiunta dell'import: {str(e)}")
        return new_data

def calculate_financial_metrics(df):
    """Calcola metriche finanziarie"""
    if df.empty:
        return {
            'total_transactions': 0, 'total_income': 0, 'total_expenses': 0, 'net_balance': 0,
            'avg_transaction': 0, 'expense_categories': 0
        }
    
    if 'importo_lordo' in df.columns:
        df['importo_lordo'] = pd.to_numeric(df['importo_lordo'], errors='coerce').fillna(0)
    
    total_transactions = len(df)
    expenses = df[df['importo_lordo'] < 0]['importo_lordo'].sum()
    income = df[df['importo_lordo'] > 0]['importo_lordo'].sum()
    net_balance = income + expenses
    avg_transaction = df['importo_lordo'].mean() if total_transactions > 0 else 0
    
    if 'categoria' in df.columns:
        unique_categories = df[df['categoria'].str.strip() != '']['categoria'].nunique()
    else:
        unique_categories = 0
    
    return {
        'total_transactions': total_transactions,
        'total_income': income,
        'total_expenses': abs(expenses),
        'net_balance': net_balance,
        'avg_transaction': avg_transaction,
        'expense_categories': unique_categories
    }

def generate_import_id(filename):
    """Genera un ID univoco per l'import"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    name_part = filename.replace('.xlsx', '').replace('.xls', '')[:20]
    return f"PP_{timestamp}_{name_part}"

def get_file_hash(uploaded_file):
    """Genera hash del file per evitare duplicati"""
    try:
        file_bytes = uploaded_file.read()
        uploaded_file.seek(0)
        return hashlib.md5(file_bytes).hexdigest()
    except:
        return str(hash(uploaded_file.name + str(uploaded_file.size)))

# === INIZIALIZZAZIONE SESSION STATE ===
if 'show_mass_categorize' not in st.session_state:
    st.session_state['show_mass_categorize'] = False
if 'show_mass_invoice' not in st.session_state:
    st.session_state['show_mass_invoice'] = False
if 'show_quick_categorize' not in st.session_state:
    st.session_state['show_quick_categorize'] = False
if 'selected_for_categorize' not in st.session_state:
    st.session_state['selected_for_categorize'] = []
if 'show_bulk_categorize' not in st.session_state:
    st.session_state['show_bulk_categorize'] = False
if 'bulk_selected_indices' not in st.session_state:
    st.session_state['bulk_selected_indices'] = []

# === HEADER PRINCIPALE ===
st.title("💰 PayPal Manager Professional")
st.markdown("### 🚀 Gestione completa estratti conto PayPal con storage GitHub")
st.markdown("---")

# Carica metadati da GitHub
with st.spinner("🔄 Caricamento dati da GitHub..."):
    metadata = load_metadata()

# === SIDEBAR - CONFIGURAZIONE ===
st.sidebar.header("⚙️ Gestione Sistema")
st.sidebar.info("💾 Dati salvati automaticamente su GitHub")

# === GESTIONE CATEGORIE ===
st.sidebar.subheader("🏷️ Gestione Categorie")
categories = metadata.get('categories', [])

# Aggiungi nuova categoria
with st.sidebar.expander("➕ Aggiungi Categoria"):
    new_category = st.text_input("Nome categoria", key="new_cat_input")
    if st.button("Aggiungi", key="add_cat_btn"):
        if new_category and new_category.strip() != "":
            new_category = new_category.strip()
            if new_category not in categories:
                categories.append(new_category)
                metadata['categories'] = categories
                if 'subcategories' not in metadata:
                    metadata['subcategories'] = {}
                metadata['subcategories'][new_category] = []
                save_metadata(metadata)
                st.success(f"✅ Categoria '{new_category}' aggiunta!")
                st.rerun()
            else:
                st.warning("⚠️ Categoria già esistente")
        else:
            st.warning("⚠️ Inserisci un nome valido")

# Mostra categorie esistenti
if categories:
    st.sidebar.write("**Categorie disponibili:**")
    for i, cat in enumerate(categories):
        col1, col2 = st.sidebar.columns([3, 1])
        with col1:
            st.text(f"• {cat}")
        with col2:
            if st.button("🗑️", key=f"del_cat_{i}", help="Elimina categoria"):
                categories.remove(cat)
                if cat in metadata.get('subcategories', {}):
                    del metadata['subcategories'][cat]
                metadata['categories'] = categories
                save_metadata(metadata)
                st.rerun()

# Storico import
import_history = metadata.get('import_history', [])
historical_df = load_historical_data()

st.sidebar.subheader("📊 Storico Import")
if import_history:
    st.sidebar.success(f"Import storici: {len(import_history)}")
    
    with st.sidebar.expander("📋 Gestisci Import"):
        for i, imp in enumerate(import_history):
            col1, col2 = st.sidebar.columns([3, 1])
            with col1:
                st.text(f"{imp['filename']}")
                st.caption(f"📅 {imp['import_date']} - {imp['total_records']} rec.")
            with col2:
                if st.button("🗑️", key=f"del_imp_{i}", help="Rimuovi import"):
                    if not historical_df.empty and 'import_id' in historical_df.columns:
                        historical_df = historical_df[historical_df['import_id'] != imp['import_id']]
                        save_historical_data(historical_df)
                    
                    import_history.remove(imp)
                    metadata['import_history'] = import_history
                    save_metadata(metadata)
                    st.success("✅ Import rimosso!")
                    st.rerun()
else:
    st.sidebar.info("Nessun import effettuato")

# Upload nuovo estratto conto
st.sidebar.subheader("📁 Carica Estratto Conto")
uploaded_file = st.sidebar.file_uploader(
    "Seleziona file Excel PayPal",
    type=['xlsx', 'xls'],
    help="Estratto conto PayPal in formato Excel"
)

if uploaded_file is not None:
    file_hash = get_file_hash(uploaded_file)
    existing_hashes = [imp.get('file_hash', '') for imp in import_history]
    
    if file_hash in existing_hashes:
        st.sidebar.warning(f"⚠️ File già caricato: {uploaded_file.name}")
    else:
        with st.spinner("🔄 Processando e salvando su GitHub..."):
            try:
                paypal_df = load_paypal_excel(uploaded_file)
                
                if paypal_df is not None:
                    import_id = generate_import_id(uploaded_file.name)
                    
                    import_metadata = {
                        'import_id': import_id,
                        'filename': uploaded_file.name,
                        'import_date': datetime.now().strftime('%Y-%m-%d'),
                        'import_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'total_records': len(paypal_df),
                        'file_hash': file_hash
                    }
                    
                    # Aggiungi allo storico
                    historical_df = add_import_to_historical(paypal_df, import_metadata)
                    
                    # Aggiorna metadati
                    import_history.append(import_metadata)
                    metadata['import_history'] = import_history
                    save_metadata(metadata)
                    
                    st.sidebar.success("✅ Estratto conto caricato e salvato!")
                    st.rerun()
                    
            except Exception as e:
                st.sidebar.error(f"❌ Errore: {str(e)}")

# === AREA PRINCIPALE ===
if not import_history:
    # Schermata di benvenuto
    st.info("👋 Benvenuto nel PayPal Manager! Carica il tuo primo estratto conto per iniziare.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🚀 **Come iniziare**
        1. **Aggiungi categorie** dalla sidebar
        2. **Carica** l'estratto conto Excel di PayPal
        3. **Modifica** direttamente i dati nella tabella
        4. **I dati vengono salvati automaticamente su GitHub**
        """)
    
    with col2:
        st.markdown("""
        ### ✨ **Funzionalità**
        - 📝 **Editor tabellare** per modifiche dirette
        - 🏷️ **Categorie personalizzabili** 
        - 📊 **Dashboard** con metriche finanziarie
        - 📈 **Grafici interattivi** per l'analisi
        - 💾 **Persistenza permanente** su GitHub
        """)
    
    st.markdown("""
    ### 🔧 **Setup GitHub (solo prima volta)**
    Per salvare i dati permanentemente, configura:
    1. Crea un repository GitHub per i dati
    2. Genera un Personal Access Token
    3. Configura i secrets in Streamlit Cloud
    """)

else:
    # Carica i dati storici
    df = historical_df.copy()
    
    if not df.empty:
        st.header("📊 Dashboard PayPal Manager")
        st.info(f"📊 {len(df)} transazioni totali • 📅 {len(df['import_id'].unique()) if 'import_id' in df.columns else 0} import • 💾 Dati salvati su GitHub")
        
        # === METRICHE PRINCIPALI ===
        metrics = calculate_financial_metrics(df)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("💳 Transazioni", f"{metrics['total_transactions']:,}")
        
        with col2:
            st.metric("📈 Entrate", f"€{metrics['total_income']:,.2f}")
        
        with col3:
            st.metric("📉 Spese", f"€{metrics['total_expenses']:,.2f}")
        
        with col4:
            st.metric("💰 Bilancio", f"€{metrics['net_balance']:,.2f}")
        
        # === TABS PRINCIPALI ===
        tab1, tab2 = st.tabs(["📝 Editor Dati", "📊 Dashboard"])
        
        with tab1:
            st.header("📝 Editor Transazioni")
            
            # Editor semplificato - mantieni la logica esistente ma con salvataggio GitHub
            if not df.empty:
                edit_columns = ['data', 'nome', 'tipo_transazione', 'importo_lordo', 'categoria', 'sottocategoria', 'tipo_spesa', 'numero_fattura', 'note']
                edit_columns = [col for col in edit_columns if col in df.columns]
                
                edit_df = df[edit_columns].copy()
                
                if 'data' in edit_df.columns:
                    edit_df['data'] = edit_df['data'].dt.strftime('%Y-%m-%d')
                
                edited_df = st.data_editor(
                    edit_df,
                    use_container_width=True,
                    height=600,
                    num_rows="dynamic",
                    column_config={
                        "categoria": st.column_config.SelectboxColumn(
                            "Categoria",
                            options=categories,
                            required=False
                        ),
                        "importo_lordo": st.column_config.NumberColumn(
                            "Importo €",
                            format="%.2f"
                        )
                    },
                    key="data_editor_github"
                )
                
                # Pulsante per salvare su GitHub
                if st.button("💾 Salva su GitHub", type="primary"):
                    try:
                        # Aggiorna i dati nello storico
                        updated_historical = historical_df.copy()
                        
                        # Logica di aggiornamento (mantieni quella esistente)
                        for idx, edited_row in edited_df.iterrows():
                            if idx < len(df):
                                original_row = df.iloc[idx]
                                
                                if 'id_univoco' in original_row:
                                    hist_mask = updated_historical['id_univoco'] == original_row['id_univoco']
                                    
                                    if hist_mask.any():
                                        for col in ['categoria', 'sottocategoria', 'tipo_spesa', 'numero_fattura', 'note']:
                                            if col in edited_row:
                                                updated_historical.loc[hist_mask, col] = str(edited_row[col])
                                        
                                        updated_historical.loc[hist_mask, 'last_modified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        # Salva su GitHub
                        save_historical_data(updated_historical)
                        
                        # Aggiorna categorie
                        new_categories = set(edited_df['categoria'].dropna().unique())
                        for cat in new_categories:
                            if cat and str(cat).strip() != "" and cat not in categories:
                                categories.append(str(cat))
                        
                        metadata['categories'] = categories
                        save_metadata(metadata)
                        
                        st.success("✅ Dati salvati su GitHub!")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Errore nel salvataggio: {str(e)}")
        
        with tab2:
            st.header("📊 Dashboard Finanziario")
            
            # Dashboard semplificata
            col1, col2 = st.columns(2)
            
            with col1:
                if 'categoria' in df.columns:
                    category_data = df[(df['importo_lordo'] < 0) & 
                                     (df['categoria'].notna()) & 
                                     (df['categoria'].str.strip() != '')]
                    
                    if not category_data.empty:
                        cat_spending = category_data.groupby('categoria')['importo_lordo'].sum().abs()
                        
                        fig_pie = px.pie(
                            values=cat_spending.values,
                            names=cat_spending.index,
                            title="🏷️ Distribuzione Spese per Categoria"
                        )
                        st.plotly_chart(fig_pie, use_container_width=True)
                    else:
                        st.info("📊 Nessuna spesa categorizzata")
                else:
                    st.info("📊 Aggiungi categorie per vedere la distribuzione")
            
            with col2:
                # Andamento mensile
                if 'data' in df.columns and not df.empty:
                    df_monthly = df.copy()
                    df_monthly['mese'] = df_monthly['data'].dt.to_period('M')
                    monthly_data = df_monthly.groupby('mese')['importo_lordo'].sum().reset_index()
                    monthly_data['mese'] = monthly_data['mese'].astype(str)
                    
                    fig_line = px.line(
                        monthly_data,
                        x='mese',
                        y='importo_lordo',
                        title="📅 Andamento Mensile",
                        markers=True
                    )
                    st.plotly_chart(fig_line, use_container_width=True)
                else:
                    st.info("📅 Dati insufficienti per l'andamento mensile")
            
            # Insights rapidi
            st.subheader("🧠 Insights Rapidi")
            
            insights = []
            
            # Insight sul bilancio
            if metrics['net_balance'] > 0:
                insights.append(f"💚 **Bilancio positivo**: €{metrics['net_balance']:,.2f}")
            elif metrics['net_balance'] < 0:
                insights.append(f"🔴 **Bilancio negativo**: €{abs(metrics['net_balance']):,.2f}")
            else:
                insights.append("⚖️ **Bilancio in pareggio**")
            
            # Insight sulla categorizzazione
            if 'categoria' in df.columns:
                total_transactions = len(df)
                categorized = len(df[df['categoria'].str.strip() != ''])
                categorization_rate = (categorized / total_transactions * 100) if total_transactions > 0 else 0
                
                if categorization_rate >= 80:
                    insights.append(f"✅ **Categorizzazione eccellente**: {categorization_rate:.1f}%")
                elif categorization_rate >= 50:
                    insights.append(f"🟡 **Categorizzazione buona**: {categorization_rate:.1f}%")
                else:
                    insights.append(f"🔴 **Categorizzazione da migliorare**: {categorization_rate:.1f}%")
            
            # Mostra insights
            if insights:
                for insight in insights:
                    st.markdown(f"• {insight}")
    
    else:
        st.warning("⚠️ Nessun dato disponibile. Carica un estratto conto per iniziare.")

# === FOOTER ===
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; font-size: 0.8em;'>
💰 PayPal Manager Professional v3.0 - GitHub Edition<br>
🚀 Storage permanente • 📊 Dashboard integrato • 💾 Backup automatico su GitHub<br>
🔗 I tuoi dati sono al sicuro nel cloud
</div>
""", unsafe_allow_html=True)
