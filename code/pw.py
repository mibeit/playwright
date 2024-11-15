import asyncio
import scrapy
from scrapy.linkextractors import LinkExtractor
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

async def handle_cookie_consent(page): 
    possible_xpaths = [
        '//*[@id="onetrust-reject-all-handler"]',
        '//*[@id="focus-lock-id"]/div[2]/div/div[2]/div/div/div[2]/div/button[2]',
        '//*[@id="iubenda-cs-banner"]/div/div/div/div[3]/div[2]/button[1]',
        '//*[@id="onetrust-reject-all-handler"]',
        '/html/body/div[6]/div/div/div[2]/span[3]/button'
    ]

    for xpath in possible_xpaths:
        button_element = await page.query_selector(f'xpath={xpath}')
        if button_element:
            await page.click(f'xpath={xpath}')
            await page.wait_for_load_state()
            break

def extract_price(text):
    # Extrahiere die Zahl im Format 100,00 oder 100.00
    match = re.search(r'\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})', text)
    if match:
        return match.group(0).replace('.', ',')
    return None

async def main():
    async with async_playwright() as p:
        # Datum des aktuellen Durchlaufs im Format DD.MM.YYYY
        current_date = datetime.now().strftime('%d.%m.%Y')

        # Liste zum Speichern der gescrapten Daten
        scraped_data = []

        # Wenn die Pricing-Datei bereits existiert, laden und überprüfen
        try:
            existing_pricing_df = pd.read_excel(pricing_path)
            if current_date in existing_pricing_df['Date'].values:
                print(f"Einträge für das Datum {current_date} existieren bereits.")
                return
        except FileNotFoundError:
            existing_pricing_df = pd.DataFrame()

        # Gruppiere den DataFrame nach Brand
        grouped = df.groupby('Brand')

        for brand, group in grouped:
            # Neuen Browser für jede Brand starten
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()

            for index, row in group.iterrows():
                # Neue Seite für jede Website öffnen
                page = await context.new_page()
                await page.goto(row['Website'])
                await asyncio.sleep(2)
                
                # Cookie-Consent auf der Seite behandeln
                await handle_cookie_consent(page)
                
                # Preis-Information extrahieren
                price_xpath = row['xpath']
                price_element = await page.query_selector(f'xpath={price_xpath}')
                if price_element:
                    price_text = await price_element.text_content()
                    price = extract_price(price_text)
                    if price:
                        # Preis in float umwandeln
                        price_float = float(price.replace(',', '.'))
                        # Gescrapte Daten speichern
                        scraped_data.append([current_date, row['Brand'], row['Productname'], price_float])
                
                # 10 Sekunden auf der Seite bleiben
                await asyncio.sleep(3)

            # Browser schließen
            await browser.close()

        # Neuen DataFrame mit den gescrapten Daten erstellen
        pricing_df = pd.DataFrame(scraped_data, columns=['Date', 'Brand', 'Productname', 'Price'])

        # Wenn die Pricing-Datei bereits existiert, aktualisieren
        if not existing_pricing_df.empty:
            pricing_df = pd.concat([pricing_df, existing_pricing_df])

        # DataFrame nach Datum sortieren und das aktuellste Datum oben anzeigen
        pricing_df = pricing_df.sort_values(by='Date', ascending=False)

        # Aktualisierten DataFrame speichern
        pricing_df.to_excel(pricing_path, index=False, engine='openpyxl')

asyncio.run(main())