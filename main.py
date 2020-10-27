import configparser
import ftplib
import os
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas
import pygsheets
import requests
import xlrd as xr

# параметры для каждого из файлов
dataset = ['name', 'sheet', 'row_number', 'sync', 'onstock', 'price']
coding = 'utf-8'


def load_config():
    """ loads config from config.cfg """
    global ftp_path, ftp_folder, ftp_user, ftp_password, rate
    config = configparser.ConfigParser()
    if os.path.exists('config.cfg'):
        config.read('config.cfg', encoding=coding)
        for item in dataset:
            if config.get('LOAD', item.lower()):
                from_dict[item] = config.get('LOAD', item)
            if config.get('SAVE', item.lower()):
                to_dict[item] = config.get('SAVE', item)
        ftp_path = config.get('FTP', 'url')
        ftp_folder = config.get('FTP', 'path')
        ftp_user = config.get('FTP', 'user')
        ftp_password = config.get('FTP', 'password')
        if config.get('EXCHANGE', 'read_NBU') == '0':
            rate = (config.get('EXCHANGE', 'sheet'),
                    config.get('EXCHANGE', 'row'), config.get('EXCHANGE', 'column'))
        else:
            rate = None


def load_xls(datadict):
    """ loads data from XLS file """
    if os.path.exists('C:\\ftp_temp\\' + datadict['name']):
        wb = xr.open_workbook('C:\\ftp_temp\\' + datadict['name'], formatting_info=False)
        try:
            ws = wb.sheet_by_name(datadict['sheet'])
            for i in range(ws.ncols):
                if ws.cell(int(datadict['row_number']), i).value == datadict['sync']:
                    id_col = i
                elif ws.cell(int(datadict['row_number']), i).value == datadict['onstock']:
                    av_col = i
                elif ws.cell(int(datadict['row_number']), i).value == datadict['price']:
                    pr_col = i
            res = {}
            for i in range(int(datadict['row_number']) + 1, ws.nrows):
                res.update({ws.cell(i, id_col).value: (ws.cell(i, av_col).value, ws.cell(i, pr_col).value)})
        except:
            print('Не найден лист в книге')
            return None
    else:
        print('Недействительный путь')
        return None
    return res


def download_xlsx():
    """ downloads XLS file from ftp server on local machine """
    ftp = ftplib.FTP(ftp_path)
    ftp.__class__.encoding = sys.getfilesystemencoding()
    print('FTP Login')
    ftp.login(user=ftp_user, passwd=ftp_password)
    print('FTP folder')
    ftp.cwd(ftp_folder)
    if not os.path.exists('C:\\ftp_temp'):
        os.mkdir('C:\\ftp_temp')
    main_dir = os.getcwd()
    os.chdir('C:\\ftp_temp')
    out = 'C:\\ftp_temp\\' + from_dict['name']
    with open(out, 'wb') as f:
        name = from_dict['name'].encode(sys.getfilesystemencoding()).decode(coding)
        get_file(ftp, name, f, coding)
    ftp.quit()
    os.chdir(main_dir)


def get_file(ftp, name, f, coding):
    try:
        ftp.retrbinary('RETR ' + name, f.write)
    except:
        print('Кодировка ' + coding + ' не подходит')


def get_rate():
    """ gets exchange rate from NBU's official resource """
    json_res = requests.get('https://bank.gov.ua/NBUStatService/v1/statdirectory/exchangenew?json')
    json_res = json_res.json()
    for item in json_res:
        if item['cc'] == 'USD':
            return item['rate']


def load_gsheet(wks, datadict):
    """ loads data from GSheet """
    # определить номера колонок по названиям
    data = wks.get_values((int(datadict['row_number']), 1), (int(datadict['row_number']), wks.cols), returnas='matrix')[
        0]
    for i in range(len(data)):
        if data[i] == datadict['sync']:
            id_col = i + 1
        elif data[i] == datadict['onstock']:
            av_col = i + 1
        elif data[i] == datadict['price']:
            pr_col = i + 1
    if (id_col == 0) | (av_col == 0) | (pr_col == 0):
        print('Не идентифицированы колонки')
        return None
    print('Идентифицированы колонки')
    ids = wks.get_values((int(datadict['row_number']) + 1, id_col), (wks.rows, id_col), returnas='matrix')
    avs = wks.get_values((int(datadict['row_number']) + 1, av_col), (wks.rows, av_col), returnas='matrix')
    prs = wks.get_values((int(datadict['row_number']) + 1, pr_col), (wks.rows, pr_col), returnas='matrix')
    ids = [item[0] for item in ids]
    avs = [item[0] for item in avs]
    prs = [make_float(item[0]) for item in prs]
    print('Считаны значения для синхронизации')
    return pandas.DataFrame({'GAvs': avs, 'GPrice': prs}, index=ids)


def make_float(price):
    try:
        price = float(price)
    except:
        # автоматическое преобразование не выполнено
        comma = price.index(',')
        if comma > 0:
            uah = int(price[:comma])
            kop = int(price[comma + 1:])
            price = uah + kop / 100
        else:
            print('Цена ' + str(price) + ' не распознана')
            price = 0
    return price


def table_for_update(df, datadict):
    """ makes DataFrame for updating GSheet """
    global no_xls
    df['XLSPrice'] = 0
    df['XLSAvls'] = 'FALSE'
    for index, row in df.iterrows():
        if index in datadict.keys():
            if datadict[index][0].strip() == 'В наличии':
                df.at[index, 'XLSAvls'] = 'TRUE'
            elif datadict[index][0].strip() == 'Нет в наличии':
                df.at[index, 'XLSAvls'] = 'FALSE'
            df.at[index, 'XLSPrice'] = rate_value * make_float(datadict[index][1])
        else:
            no_xls.append(index)
    for item in datadict.keys():
        if item not in list(df.index):
            no_gsheets.append(item)
    print('Сформирована таблица обновлений')
    return df


def sendreport(start, finish):
    """ sends report via GMail """
    global no_xls, no_gsheets
    mail_message = 'Следующие коды товара отсутствуют в XLS файле: '
    mail_message += ', '.join(no_xls)
    mail_message += '\n'
    mail_message += 'Следующие коды товара отсутствуют в таблице Google: '
    mail_message += ', '.join(no_gsheets) + '\n\n' + 'Начало синхронизации ' + str(
        start) + '\n' + 'Конец синхронизации ' + str(finish) + '\n'

    # The mail addresses and password
    sender_address = '***@gmail.com'
    sender_pass = '***'
    receiver_address = '***@gmail.com'
    # Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Отчет от синхронизации таблиц'  # The subject line
    # The body and the attachments for the mail
    message.attach(MIMEText(mail_message, 'plain'))
    # Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587)  # use gmail with port
    session.starttls()  # enable security
    session.login(sender_address, sender_pass)  # login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()
    print('Сообщение отправлено')


def main():
    global rate_value
    start = datetime.now()
    load_config()
    print('Загружен конфигурационный файл')
    download_xlsx()
    print('Загружен XLSX файл')
    xls_dict = load_xls(from_dict)
    if xls_dict:
        gc = pygsheets.authorize()
        sh = gc.open(to_dict['name'])
        wks = sh.worksheet_by_title(to_dict['sheet'])
        # курс доллара
        if not rate:
            rate_value = get_rate()
        else:
            wks_rate = sh.worksheet_by_title(rate[0])
            rate_value = wks_rate.get_value((int(rate[1]), int(rate[2])))
            try:
                rate_value = float(rate_value)
            except:
                # стандартное преобразование не сработало
                rate_value = float(rate_value.strip().replace(',', '.'))
        df = load_gsheet(wks, to_dict)
        updf = table_for_update(df, xls_dict)
        avl_update = [[item] for item in list(updf['XLSAvls'])]
        price_update = [[item] for item in list(updf['XLSPrice'])]
        avl_range = pygsheets.datarange.DataRange(start=(3, 5), end=(wks.rows, 5), worksheet=wks)
        avl_range.update_values(values=avl_update)
        price_range = pygsheets.datarange.DataRange(start=(3, 7), end=(wks.rows, 7), worksheet=wks)
        price_range.update_values(values=price_update)
        print('Записаны изменения')
    finish = datetime.now()
    print('Отправка сообщения о синхронизации')
    sendreport(start, finish)
    print('Начало работы ' + str(start))
    print('Конец работы ' + str(finish))


if __name__ == '__main__':
    # создать необходимые типы данных
    from_dict, to_dict = {}, {}
    no_gsheets, no_xls = [], []
    ftp_path, ftp_folder, ftp_user, ftp_password = '', '', '', ''
    rate = ''
    rate_value = 0;
    main()

