# coding=utf-8
import json
import os

import pandas as pd

project_root = os.path.dirname(os.path.realpath(__file__))
user_config_file_name = os.path.join(project_root, "config", "default.json")


def default_parameters():
    return dict(db_user_name='ibs', db='p2', project_directory=texts_working_dir,
                oracle_home=os.environ["ORACLE_HOME"])


def write_parameters(cfg):
    if not os.path.exists(os.path.dirname(user_config_file_name)):
        os.makedirs(os.path.dirname(user_config_file_name))
    with open(user_config_file_name, 'w+') as f:
        f.write(json.dumps(cfg))


def read_parameters():
    if os.path.isfile(user_config_file_name):
        with open(user_config_file_name, 'r') as f:
            cfg = json.load(f)

    else:
        cfg = default_parameters()
        write_parameters(cfg)
    return cfg


git_url = "http://git.brc.local:3000/ivan.bryzzhin/abs.git"
git_folder = r"C:\Users\BryzzhinIS\Documents\Хранилища\sync_script\dbs"
texts_working_dir = r"C:\Users\BryzzhinIS\Documents\Хранилища\pack_texts"

if "ORACLE_HOME" not in os.environ:
    os.environ["ORACLE_HOME"] = read_parameters().get('oracle_home')
os.environ['NLS_LANG'] = '.AL32UTF8'

# кол-во дней от текущей даты на базе
# которое учитывается чтобы обновить объект
# например по событию компиляции
# объекты старше сохраняться по компиляции не будут
days_when_object_modified_update_from_action = 30
# кол-во дней, за которое грузятся обновленные объекты
# при перезапуске джоба
days_update_on_start = 7

# список пользователей
# операции которых сохраняются в гит
users_to_save_objects_in_git = [
    'BryzzhinIS',
    'DoblerEA',
    # 'Kirpichnikov',
    'SysovaEA',
    'UrypinaAV',
    'kashapovar',
    'matveevsa',
    'pichuginain',
    'shavkunov',
    'tihonoviv',
    'Tyrykina',
    'Morozovasi',
]
users_to_save_objects_in_git_from_select_by_diaries = ['AUDM', 'AbakulovaVV', 'AbraimovIA', 'AbydenovMD', 'AchkanovAA',
                                                       'AdamovichTV', 'AhmetsafenRA', 'AmelkinaAA', 'AntonovaEM',
                                                       'BOLOTOVANA', 'BUZUNOVDY', 'BeljanskajaEV', 'BelousovDD',
                                                       'BelovAI', 'BerezkaAK', 'BezrukavajaOM', 'BolotovaNA',
                                                       'Borovkovaao', 'BoykovaYuP', 'BryzzhinIS', 'BudkoKV',
                                                       'BuluchevskyIV', 'BushinaDV', 'CherkasovaNV', 'ChuninaTA',
                                                       'CvetkovaTS', 'DOSTUP2', 'DROKOVA_TN', 'DautovaOV', 'DavudovaEN',
                                                       'DoblerEA', 'Dotsenkoos', 'Dudkoda', 'DzhankovichIV',
                                                       'Efimovavu', 'FALKO_AP', 'Fadeevaay', 'FedoseevSS', 'Galstyanae',
                                                       'Gazimagomedova', 'GildenblatGA', 'GolubitskayaLS',
                                                       'GolyanovaNP', 'GorbunovaMS', 'GorbunovaON', 'GrishinNM',
                                                       'GrozdovaIV', 'Grudovanv', 'GusevaEI', 'HamutskayaYG',
                                                       'HlebnikovaEG', 'IBS', 'IBSO_SPR', 'IgnatiadiSP', 'Isaevasa',
                                                       'IvanovED', 'IvanovaES', 'IvolgaMS', 'JOB', 'JukovaAK',
                                                       'KUCHINAEV', 'KUDRINATA', 'KashkinTO', 'KhokhlovaAA',
                                                       'KhokhlovaVA', 'KhomenkoEE', 'Kitunzid', 'KomyakovaMV',
                                                       'KorkinOA', 'KornilovaVE', 'Korovkinatyu', 'KortikovaIG',
                                                       'KostinaNA', 'Kotelkovao', 'KovalenkoDA', 'KovalenkoIS',
                                                       'Krivokorytovas', 'KudrinaTA', 'KupenkoMI', 'KurnikovaAV',
                                                       'KurochkaAA', 'Kurochkin-adm', 'KuzkinMA', 'KuznetsovaOA',
                                                       'LAPSHINOVMA', 'LantratovAV', 'LebedevaAR', 'Lebedevav',
                                                       'ListevaEA', 'Livadinazi', 'Lomteves', 'LukashevIA',
                                                       'LutsenkoKA', 'LutsenkoVP', 'MARKOVAMV', 'MSB-CRM-SVC',
                                                       'MalakhovIA', 'MaltsevND', 'Matkovaaa', 'MikheyevPA',
                                                       'Mikhinaya', 'MinkinSA', 'MorgunovaAS', 'Morozovasi',
                                                       'MullakhmetovaVF', 'MyagkovaAV', 'NAUMOVANV', 'NasretdinovaZI',
                                                       'NedobuginaAV', 'Nikolaevaei', 'NimychAS', 'OkoemovaES',
                                                       'OrlovaNM', 'OvdinaII', 'PESTROVSKAYAEV', 'PanteleevaIA',
                                                       'PereklitskayaVV', 'PestrovskayaEV', 'PetrovaEN', 'PodyachevMA',
                                                       'PyshkinaMV', 'ROSTOVTCEVA_MN', 'RYABKOVALA', 'RetuyevAV',
                                                       'Retuyevav', 'RostovcevaMN', 'RumyantsevaAI', 'RyabkovaLA',
                                                       'SAVOSTYANOVAOA', 'SAVOSTYANOVA_SF', 'SHAKIROVRV', 'SHUDRIKIA',
                                                       'SIROTKINA', 'SKOPICHME', 'SOTNIKOVA_DY', 'SVISTUNOVANA',
                                                       'SaakyanAS', 'SadaevaST', 'SavenkovAA', 'SavostjanovaOA',
                                                       'SerdyukovAS', 'ShabrovaEV', 'ShadrinaIA', 'ShakirovRV',
                                                       'ShargaevaEA', 'ShemshurAA', 'ShmidtES', 'ShtahAV', 'ShudrikIA',
                                                       'SimonenkovaOG', 'SirotkinaTV', 'Skachkovati', 'SmirnovVV',
                                                       'SokolovaSV', 'Sokolovasv', 'SolovyevSS', 'SorokinaNN',
                                                       'SvistunovaNA', 'SyrtsovaNA', 'SysovaEA', 'TUMANOVADV',
                                                       'TarabrinAYu', 'TkachenkoAY', 'TretjakovaSS', 'TukayevRF',
                                                       'TumanovaDV', 'TumanovaDv', 'Tyrykina', 'UrypinaAV',
                                                       'UsluginaKG', 'UvarovaYO', 'VASKOVAEY', 'ValdaevaMA', 'Vasenina',
                                                       'VaskovaEY', 'VetrovaLL', 'VikerevaVS', 'VinnikovaOV',
                                                       'WRUDC1AP039$', 'YakubovAN', 'YegazaryanOE', 'YevtikheyevaYeA',
                                                       'ZAHAROVA_YU', 'ZUDILOVANV', 'ZaharovaYE', 'ZherebtsovaEA',
                                                       'ZhigulinaAV', 'Zhukovavv', 'ZnamenskiyVV', 'ZudilovaNV',
                                                       'ZverevDA', 'achkanovaa', 'ahmetsafenra', 'antonovmv',
                                                       'artishchevaas', 'async', 'badurashvilinl', 'bapaas',
                                                       'bayramovarz', 'belousas', 'belousovDD', 'belousovdd',
                                                       'belozerovek', 'berezkaak', 'berlinaea', 'brinevama',
                                                       'bukanovaeg', 'buluchevskiiav', 'buzunovdy', 'bystrova',
                                                       'chechulinalv', 'cherkasovanv', 'chernyavskayaas', 'danilovaks',
                                                       'dautovaov', 'davudovaen', 'dianovaa', 'dimitrovads',
                                                       'dmitrievadg', 'dmokhev', 'dokuchaeva', 'dotsenkoos',
                                                       'drobotenkoov', 'drokovatn', 'druzhininanv', 'durandinaad',
                                                       'dzhankovichiv', 'efimkinadi', 'falkoap', 'fedorinayd',
                                                       'finogenovaea', 'fomichvv', 'furmanoa', 'fxgate',
                                                       'gabdrafikovri', 'galkinaos', 'galstyanae', 'gribinikovang',
                                                       'grinbaumae', 'gruzdevana', 'hayrapetyanmm', 'ibs', 'isaevasa',
                                                       'ivanovaev', 'iya', 'kalininakd', 'kamnevaeg', 'karkanitsavn',
                                                       'kashkinto', 'kasyanovvv', 'katleevasg', 'kazantsevamv',
                                                       'khmurchikva', 'khovrychevakg', 'khudoyarovaya', 'kirilichevamv',
                                                       'kokiashvilioa', 'kolomeytsevaes', 'komendatay', 'kondaurovanv',
                                                       'korovkinatyu', 'koshelevva', 'kovalenkoda', 'kovrovnn',
                                                       'krenevayd', 'krylovams', 'kst-audit-1', 'kst-audit-10',
                                                       'kst-audit-2', 'kst-audit-3', 'kst-audit-4', 'kst-audit-5',
                                                       'kst-audit-6', 'kst-audit-7', 'kst-audit-8', 'kst-audit-9',
                                                       'kuchinaev', 'kudrinamg', 'kupriyanovVO', 'kuvaldinasv',
                                                       'lapshinovama', 'lazarevaem', 'lebedevada', 'legezinaei',
                                                       'lesnih', 'listevaEA', 'litovchenkois', 'livadinazi', 'lomteves',
                                                       'lukashevia', 'lukyanovsb', 'maleckihes', 'marchenkoaa',
                                                       'markovamv', 'marmuzovav', 'martsinkevichev', 'mashkovayv',
                                                       'matkovaaa', 'matveevaav', 'matveevsa', 'matveyevayed',
                                                       'matyukhinaaa', 'molinms', 'molodtsovaoa', 'morozovasi',
                                                       'mozhinavju', 'neumyvakinaso', 'novozhilovaiu', 'oleinikmk',
                                                       'oracle', 'panchenkoad', 'perminayy', 'pichuginain', 'politovsv',
                                                       'popovaea', 'popovave', 'pospelovaas', 'prokopenkoai', 'radkoen',
                                                       'romanovvv', 'roshchinaen', 'rulenkovv', 'ryabkovala',
                                                       'sakharovaayu', 'salagaevako', 'savostjanovaoa', 'semenov-adm',
                                                       'shavkunov', 'shchegolkovav', 'shestelye', 'shevchikia',
                                                       'shpilmanev', 'shvetsne', 'shvetsovayy', 'sitnovas',
                                                       'skachkovati', 'skopichme', 'smirnovaiv', 'smirnovaov',
                                                       'sokolovasv', 'sokolovave', 'sotnikovadyu', 'surovtsevana',
                                                       'sutyaginamp', 'sviridovaia', 'sychevaas', 'tanzybaevae',
                                                       'tereshchenkoea', 'tihonoviv', 'timofeevaoo', 'titovamm',
                                                       'toroshchinanv', 'trekhsvyatskayamv', 'tumzovaen',
                                                       'tverdokhlebovaom', 'tyapkinase', 'tyrykina', 'urypinaav',
                                                       'valyaevea', 'vasilchenkoEA', 'veremeichikus', 'vinnikovaov',
                                                       'vodolazkiinr', 'web', 'yarushnikovaoa', 'yegazaryanOE',
                                                       'zabbix', 'zaicevatp', 'zakharovaai', 'zakharovayn',
                                                       'zakharovyn', 'zhukovavv']
users_to_save_objects_in_git += users_to_save_objects_in_git_from_select_by_diaries
users_to_save_objects_in_git_str = ','.join(["'%s'" % a.upper() for a in users_to_save_objects_in_git])

pd.options.display.width = 300
pd.options.display.max_rows = None

dbs = {
    # 'ass': "ibs/HtuRhtl@lw-ass-abs",
    # 'abs': "ibs/HtuRhtl@lw-abs-abs",
    # 'p2': "ibs/HtuRhtl@lw-p2-abs",
    # 'msb': "ibs/HtuRhtl@msb",
    # 'mid': "ibs/HtuRhtl@midabs",
    # 'midday': "ibs/HtuRhtl@MIDEVERYDAY",
    'ass': "ibs/HtuRhtl@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = lw-ass-abs.brc.local)(PORT = 1521)))(CONNECT_DATA =(SID = assabs)))",
    'abs': "ibs/HtuRhtl@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = lw-abs-abs.brc.local)(PORT = 1521)))(CONNECT_DATA =(SID = lwabsabs)))",
    'p2': "ibs/HtuRhtl@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = lw-p2-abs.brc.local)(PORT = 1521)))(CONNECT_DATA =(SID = lwp2abs)))",
    'msb': "ibs/HtuRhtl@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = lw-abs-abs-msb.brc.local)(PORT = 1521)))(CONNECT_DATA =(SID = msb)))",
    'mid': "ibs/HtuRhtl@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = mid-abs.brc.local)(PORT = 1521)))(CONNECT_DATA =(SID = midabs)))",
    'midday': "ibs/HtuRhtl@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 172.21.13.152)(PORT = 1521)))(CONNECT_DATA =(SID = Midabsev)))",
    'day': "ibs/HtuRhtl@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = lw-abs-abs-everyday)(PORT = 1521)))(CONNECT_DATA =(SID = lwabsev)))",
}
