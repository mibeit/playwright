import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import re
from datetime import datetime

# Pfad zur ursprünglichen Excel-Datei
excel_path = 'excel/dummy.xlsx'

# Pfad zur neuen Excel-Datei
pricing_path = 'excel/pricing.xlsx'

# Ursprüngliche Excel-Datei in einen DataFrame laden
df = pd.read_excel(excel_path)

async def handle_cookie_consent(page, consent_xpaths):
    for xpath in consent_xpaths:
        if pd.notna(xpath):  # Überprüfen, ob der XPath nicht leer ist
            button_element = await page.query_selector(f'xpath={xpath}')
            if button_element:
                await page.click(f'xpath={xpath}')
                await page.wait_for_load_state()

def extract_price(text):
    # Extrahiere die Zahl im Format 100,00 oder 100.00 oder 100,
    match = re.search(r'\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?', text)
    if match:
        price = match.group(0)
        if price.endswith(','):
            price = price[:-1]  # Entferne das letzte Komma, falls vorhanden
        # Ersetze Punkt durch Komma, falls vorhanden
        price = price.replace('.', ',')
        return price 
    return None

async def process_page(context, row, current_date, scraped_data, failed_entries):
    try:
        # Neue Seite öffnen
        page = await context.new_page()
        await page.goto(row['Website'])
        await asyncio.sleep(2)
        
        # Cookie-Consent auf der Seite behandeln
        consent_xpaths = [row['first_content_xpath'], row['second_content_xpath']]
        await handle_cookie_consent(page, consent_xpaths)
        
        # Preis-Information extrahieren
        price_xpath = row['price_xpath']
        price_element = await page.query_selector(f'xpath={price_xpath}')
        if price_element:
            price_text = await price_element.text_content()
            price = extract_price(price_text)
            if price:
                # Gescrapte Daten speichern
                scraped_data.append([current_date, row['Brand'], row['Productname'], price])
        else:
            raise ValueError("Preiselement konnte nicht gefunden werden.")
        
        # 10 Sekunden auf der Seite bleiben
        await asyncio.sleep(3)
    except Exception as e:
        # Fehler protokollieren und den fehlgeschlagenen Eintrag speichern
        print(f"Fehler bei {row['Brand']} - {row['Productname']}: {e}")
        failed_entries.append([row['Brand'], row['Productname'], str(e)])
    finally:
        await page.close()

async def main():
    async with async_playwright() as p:
        # Datum des aktuellen Durchlaufs im Format DD.MM.YYYY
        current_date = datetime.now().strftime('%d.%m.%Y')

        # Liste zum Speichern der gescrapten Daten
        scraped_data = []

        # Liste zum Speichern der fehlgeschlagenen Einträge
        failed_entries = []

        # Wenn die Pricing-Datei bereits existiert, laden
        try:
            existing_pricing_df = pd.read_excel(pricing_path)
        except FileNotFoundError:
            existing_pricing_df = pd.DataFrame()

        # Gruppiere den DataFrame nach Brand
        grouped = df.groupby('Brand')

        tasks = []
        for brand, group in grouped:
            # Neuen Browser für jede Brand starten
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()

            for index, row in group.iterrows():
                tasks.append(process_page(context, row, current_date, scraped_data, failed_entries))

            await asyncio.gather(*tasks)
            await browser.close()

        # Neuen DataFrame mit den gescrapten Daten erstellen
        new_pricing_df = pd.DataFrame(scraped_data, columns=['Date', 'Brand', 'Productname', 'Price'])

        # Aktualisieren oder Hinzufügen der neuen Daten zu den bestehenden Daten
        if not existing_pricing_df.empty:
            # Entferne alte Einträge für das aktuelle Datum
            existing_pricing_df = existing_pricing_df[existing_pricing_df['Date'] != current_date]
            # Füge neue Einträge hinzu
            pricing_df = pd.concat([existing_pricing_df, new_pricing_df])
        else:
            pricing_df = new_pricing_df

        # DataFrame nach Datum sortieren und das aktuellste Datum oben anzeigen
        pricing_df = pricing_df.sort_values(by='Date', ascending=False)

        # Aktualisierten DataFrame speichern
        pricing_df.to_excel(pricing_path, index=False)

asyncio.run(main())