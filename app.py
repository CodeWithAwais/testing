import requests
import time
import os
import re
import csv
import scrapy
from tqdm import tqdm
from datetime import datetime


current_date = datetime.now().strftime('%Y%m%d')


def write_to_csv(filename, data):
    file_exists = os.path.exists(filename)
    with open(filename, 'a+', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

def scrape():
    products_list = []
    codes = open('codes.txt').read().splitlines()
    for code in tqdm(codes):
        headers = {
            'authority': 'www.carrefouruae.com',
            'accept': '*/*',
            'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-GB;q=0.6,en-US;q=0.5,sv;q=0.4,it;q=0.3,es;q=0.2',
            'appid': 'Reactweb',
            'cache-control': 'no-cache',
            'cookie': 'maf-session-id=d806a469-e765-4cd1-a9f5-6bc8ab820419; storeInfo=mafuae|en|AED; mafuae-preferred-delivery-area=Dubai Festival City - Dubai; prevAreaCode=Dubai Festival City - Dubai; cart_api=v2; _gcl_au=1.1.1042974149.1681762192; _scid=8f1c5338-a750-46f6-9f00-046ae2a6d58a; _sctr=1%7C1681758000000; guest-session-token=6d_qonQVWbN6V6qo2GmRLdfXV78; AKA_A2=A; _abck=B548DF979AAF0CA2CDA13069FB09410A~-1~YAAQvIcQAr2M7VmHAQAAZ7n/mwkqhYCAflcVELWS9K+ch9psD4wcZ5VgbW48XJu0PLN811HmYvlPVmDQS4EarqVjLOPYYTZyoyn2TxkcXshALnlnBKqqbaPLwAr7toUdtGb5CdwN1X0ClKv+8SR58CvEmgQ4ZffL4ILOHrMnx+WYsD3g/AoYZ70NXt8zR3ij/euknUhr+z/IdPRFNoA/cPuxdmLlwnMQ252V3JZOFJgfF5v1s17LDp4oj0b0Bod6j19+mvKNgSsZYpHfMfl/jVuMHhECdQmtM3TxuHwYZ5aOz/JFFJIdtfwo+29XNhWUdASG2e5Pp10VuBQ3wKV//rDXTSa4BN/p1aw8vGo2qpj5hg9HNdGcyEPk172z7ggRNZPM9364/vAbrN37vvS4uw==~-1~-1~-1; bm_sz=A872D0FAE3F99B5323647AF56B1E32DD~YAAQvIcQAsCM7VmHAQAAZ7n/mxOSiTJP0GGhCHiNhXKn4KbjCbD+U8W1aDeRxta3NSdaJnEavP2vdHuzsOjljO0OgkLLr9kL2V8BMcbcwD1y0uFSy7fB5qIMiuy9SIL48P3DG5CalvIfPq83L47EPdK1DbLTmd4k4LWYgmEQNru8MX/PX/9LtWXPX3srHrjQ/+dJiRC8/5QEq8u87OgQWh2nEVUA4xQbh7VLc7JRkUhADELWHWEt8cWB6Up65Mq6zge+MAHh+zqwAersJZZjnxTZZad1i2VTpbUMV4BsLLVZV1iicdk2/8U=~4274483~3487032; ak_bmsc=3974B6B978FD68F5EB99B4F185222730~000000000000000000000000000000~YAAQvIcQAueM7VmHAQAAr8H/mxOsfT9rvuGr78P7orxmKL9t59tgFT3IZ0va2Mo16K+hRj0aUO/E8JA173oj0lX4vxngKicRf39WrkdPiHffs/Da02jxEmgSBefZyu0jGatEfNPCQq8KMSzS4Qwbwz4R+Mo8Zf1wvRSiZVdhsiU4dk5zEIGjCEWG85ZhCrKgRALvlInS64zYpXJaRi/s8loufEc8a//gikPA1wyoneW6QHHOziKuk2Wo82E44uz4y56hFMPANcDPjKi+6dluF8fZuclz5APom+jvo4b1YRcd10fSpxvb2vYmwmjSX7mSHOfE/0X1LBMbTW1wn5BhfsRXNrqCjUF4JxidnmXsoDDvSIpoLiSlerQix9E9RTU1tw+D6yvOO38HPSuDEmuX/R8uYp/S/EmdJ545IstinHFdc2VgkjLN8PISgZ8NYHngi/2oo+H72fIcynAyWTy0OC4Z1D9IdMVAK//8Z/3UoeSq6Tox681Q8JOvJ38IiddyPw==; app_version=v4; page_type=categorySearch; SL_C_23361dd035530_VID=8qmdsm6hsJHWSg4TAtp7k; SL_C_23361dd035530_KEY=bba452972c632f2d14751e8773a06f5b8365a6fa; TEAL=v:318790d68ce797462816304938744516f6465677bb8$t:1681951245211$sn:2$en:1$s:1681949445209%3Bexp-sess; _scid_r=8f1c5338-a750-46f6-9f00-046ae2a6d58a; _ga=GA1.2.1199134775.1681762193; _gid=GA1.2.1953060455.1681949445; RT="z=1&dm=www.carrefouruae.com&si=ae56c757-053b-4ccd-ad0c-c9456c992ac0&ss=lgod9nml&sl=1&tt=6ns&rl=1&ld=6nu"; SL_C_23361dd035530_SID=ZPFWAmNXIGq_RQGLMZHAB; JSESSIONID=C395DC8B1C1E4C8F92DE348EEC2A9D1F.accstorefront-759b54c6d4-gwjgc; mafuae-web-preferred-delivery-area="Dubai Festival City - Dubai"; ROUTE=.accstorefront-759b54c6d4-gwjgc; bm_sv=C8FC3CDCF4583A1E068DFF537401ABE5~YAAQvIcQAvGU7VmHAQAAskEBnBPPYtc+fobvPhZZWUeo8Mo/a16gBWoZ/9ryh6WX5CP8qawa44mH4qpI9tTFn1dKww9lx/WGU70tikyAaO803dy6QO6LqfmdmA1XOIND0x/SDhgbGFcMWdawJkvcp1afV/uH6n0x6f7l6Og/v63NNzmEkWmjsHW3HuJ0fK1foonJNVoCuqFBwuY7+hRPFn3hkvb2lFD9pQQFJAShEiLW6ksXVnjRZGNhAI+6cMcvql6QiJ7q~1; _gat_UA-125827987-1=1; cto_bundle=G31Wel90eUFnbHJ5c2w2VGFJRGhZdXlHNE81TmdVcThvRyUyQlFXRE16JTJGYXZLbnhLRVRqcGh2ZnY5dk9QWDNLVmhrcCUyRkNoJTJCRUhDdlBNWFBWJTJCcHlpTXp1MDl2VjJrbmZWdnphN0hRMDBodm5NanFGeEtWTDlwMFZ3bCUyQk5sRyUyRjFTUCUyRmxqOXk0bjdqbCUyQnEyRGtHT1dsM2tjQmsyQXp1YmpZdVVwJTJGamJJaSUyRjlMWXVVbm44JTNE; _ga_BWW6C6N1ZH=GS1.1.1681949445.2.0.1681949547.54.0.0',
            'credentials': 'include',
            'deviceid': '1199134775.1681762193',
            'env': 'prod',
            'intent': 'STANDARD',
            'pragma': 'no-cache',
            'referer': f'https://www.carrefouruae.com/mafuae/en/c/{code}',
            'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'service': 'algolia',
            'storeid': 'mafuae',
            'token': 'undefined',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
            'userid': 'undefined',
        }
        
        counter = 0
        while True:
            params = {
                'filter': '',
                'sortBy': 'relevance',
                'currentPage': str(counter),
                'pageSize': '60',
                'maxPrice': '',
                'minPrice': '',
                'areaCode': 'Dubai Festival City - Dubai',
                'lang': 'en',
                'displayCurr': 'AED',
                'latitude': '25.2171003',
                'longitude': '55.3613635',
                'nextOffset': '',
                'needVariantsData': 'true',
                'requireSponsProducts': 'true',
                'responseWithCatTree': 'true',
                'depth': '3',
            }

            response = requests.get(
                url=f'https://www.carrefouruae.com/api/v8/categories/{code}',
                params=params,
                headers=headers,
                timeout=100,
            )
            
            products = response.json().get('products')
            if not products or products == []:
                break
            for product in products:
                link = product.get('links').get('productUrl').get('href')
                href = f'https://www.carrefouruae.com{link}'
                products_list.append(href)
                print(href)
            
            counter += 1
            print(f'SCRAPING PAGE NUMBER :: {counter}')
            
            
    for each in tqdm(products_list):
        r = requests.get(url=each, headers=headers, timeout=100)
        
        response = scrapy.Selector(text=r.text)
        json_data = response.xpath('normalize-space(//script[@id="__NEXT_DATA__"])').get()
        
        time_stamp = int(time.time())
        
        categories = re.findall(r'"categories":\[(.*?)\]', json_data)
        category, *sub_categories = re.findall(r'"name":"(.*?)"', str(categories))
        
        images = response.xpath('//div[@class="swiper-wrapper"]//div[contains(@style, "cursor")]/img/@src').getall()
        product_name = re.findall(r'"title":"(.*?)"', json_data)
        price = re.findall(r'original":{"value":"(.*?)"', json_data)
        unit = re.findall(r'units":"(.*?)"', json_data)
        
        data = {
            "Timestamp": time_stamp,
            "Category": category.replace(r'\\u0026', '&'),
            "Sub Categories": ', '.join(sub_categories).replace(r'\\u0026', '&'),
            "Images": ', '.join(images),
            "Product Link": r.url,
            "Product Name": product_name[0] if product_name else '',
            "Product Price": price[0] if price else '',
            "Product Unit": unit[0] if unit else '',
            "Discount": '',
            "Discount Price": '',
        }
        
        write_to_csv(f'carrefouruae_{current_date}.csv', data)
        print(data)

    # converting csv file to excel
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    with open(f'carrefouruae_{current_date}.csv', 'r', encoding='utf-8') as f:
        for row in csv.reader(f):
            ws.append(row)
    wb.save(f'carrefouruae_{current_date}.xlsx')

scrape()
