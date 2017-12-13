from __future__ import absolute_import, division, print_function, \
    with_statement
import requests
from peewee import * 
import json
import numpy
import logging
from multiprocessing.dummy import Pool as ThreadPool
import threading

objects_done = 0
objects_total = 0

def parse_json_save_to_sqlite(json_string):
    try:
        json_obj = json.loads(json_string)
    except Exception as e:
        logging.error(e)
        logging.error("there is some problem with json loading")
    
    total = None
    try:
        total = len(json_obj['features'])
    except Exception as e:
        logging.error(e)
        logging.error("There is no features here")
    
    obj_features = None
    try:
        obj_features = json_obj['features']
    except Exception as e:
        logging.error(e)
        logging.error("There is no features here")

    try:
        if int(total) > 0:
            print("------------- get total: %s properties ----------------" % str(total))
        else:
            pass
    except Exception as e:
        pass

    for index, each_building in enumerate(obj_features):
        building_info = None
        try:
            building_info = each_building['properties']
        except Exception as e:
            logging.error(e)
            logging.error("There is no properties here")
        
        building = PropertyModel()
        if len(str(building_info['wobj_obj_id'])) > 0:
            try:
                building.identificatie = building_info['wobj_obj_id']
            except Exception as e:
                print("this building has no identification code. ")
                building.identificatie = "none"
        else:
            building.identificatie = "none"
            print("this building has no identification code. ")
            
        if len(str(building_info['wobj_huisnummer'])) > 0:
            try:
                building.house_number = building_info['wobj_huisnummer']
            except Exception as e:
                building.house_number = "none"
                print("this building has no house number. ")
        else:
            building.house_number = "none"
            print("this building has no house number. ")
        
        if len(str(building_info['wobj_huisletter'])) > 0:
            try:
                building.house_number_ext = building_info['wobj_huisletter']
            except Exception as e:
                building.house_number_ext = "none"
                print("this building has no house number extension")
        else:
            building.house_number_ext = "none"
            print("this building has no house number extension")
            
        if len(str(building_info['wobj_postcode'])) > 0:
            try:
                building.postcode = building_info['wobj_postcode']
            except Exception as e:
                building.postcode = "none"
                print('this building has no postcode. ')
        else:
            building.postcode = "none"
            print('this building has no postcode. ')
            
        if len(str(building_info['wobj_woonplaats'])) > 0:
            try:
                building.plaatsnaam = building_info['wobj_woonplaats']
            except Exception as e:
                building.plaatsnaam = "none"
                print("this building has no plaatsnaam. ")
        else:
            building.plaatsnaam = "none"
            print("this building has no plaatsnaam. ")
        
        if len(str(building_info['wobj_straat'])) > 0:
            try:
                building.street = building_info['wobj_straat']
            except Exception as e:
                building.street = "none"
                print("this building has no street name. ")
        else:
            building.street = "none"
            print("this building has no street name. ")
            
        if len(str(building_info['wobj_bag_bouwjaar'])) > 0:
            try:
                building.bouwjaar = building_info['wobj_bag_bouwjaar']
            except Exception as e:
                building.bouwjaar = "none"
                print("this building has no bouwjaar. ")
        else:
            building.bouwjaar = "none"
            print("this building has no bouwjaar. ")
            
        if len(str(building_info['wobj_bag_gebruiksdoel'])) > 0:
            try:
                building.gebruiksdoel = building_info['wobj_bag_gebruiksdoel']
            except Exception as e:
                building.gebruiksdoel = "none"
                print("this building has no gebruiksdoel. ")
        else:
            building.gebruiksdoel = "none"
            print("this building has no gebruiksdoel. ")
            
        if len(str(building_info['wobj_oppervlakte'])) > 0:
            try:
                building.oppervlakte = building_info['wobj_oppervlakte']
            except Exception as e:
                building.oppervlakte = "none"
                print("this building has no oppervlakte. ")
        else:
            building.oppervlakte = "none"
            print("this building has no oppervlakte. ")
        
        
        building.price_2015, building.price_2016 = \
            parse_each_property_price(scrape_each_property_price(building.identificatie))
        
        try:
            building.save()
        except Exception as e:
            logging.error(e)
            logging.error("Got a duplicate record")
        
#         print("Saving property to database now, process: {:2.2f}% ...".format((index+1)*100/(total)), end="\r")


class BaseModel(Model):
    class Meta:
        database = SqliteDatabase("netherland_properties.db")
class PropertyModel(BaseModel):
    identificatie = CharField(unique=True)
    house_number = CharField(null=True)
    house_number_ext = CharField(null=True)
    postcode = CharField(null=True)
    plaatsnaam = CharField(null=True)
    street = CharField(null=True)
    
    price_2015 = CharField(null=True)
    price_2016 = CharField(null=True)
    
    bouwjaar = CharField(null=True)
    gebruiksdoel = CharField(null=True)
    oppervlakte = CharField(null=True)


def init_database():
    db = SqliteDatabase("netherland_properties.db")
    db.connect()
    try:
        db.drop_tables([PropertyModel])
    except:
        pass
    db.create_tables([PropertyModel])
    db.close()


def scrape_obj_from_id_to_id(f=None, t=None):
    if f is None:
        raise Exception("From id is None, check the script please. ")
    if t is None:
        raise Exception("To id is None, check the script please. ")
        
    # format from_id and to_id
    from_id = '%012d'%f
    to_id = '%012d'%t
    
    # initiate a request object
    s = requests.Session()
    s.get("https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&")
    
    xml_obj = \
    """
    <wfs:GetFeature
        xmlns:wfs="http://www.opengis.net/wfs" service="WFS" version="1.1.0" xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <wfs:Query typeName="wozloket:woz_woz_object" srsName="EPSG:28992"
            xmlns:WozViewer="http://WozViewer.geonovum.nl"
            xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:Filter
                xmlns:ogc="http://www.opengis.net/ogc">
                <ogc:And>
                    <ogc:PropertyIsGreaterThan matchCase="true">
                        <ogc:PropertyName>wobj_obj_id</ogc:PropertyName>
                        <ogc:Literal>%s</ogc:Literal>
                    </ogc:PropertyIsGreaterThan>
                    <ogc:PropertyIsLessThan matchCase="true">
                        <ogc:PropertyName>wobj_obj_id</ogc:PropertyName>
                        <ogc:Literal>%s</ogc:Literal>
                    </ogc:PropertyIsLessThan>
                </ogc:And>
            </ogc:Filter>
        </wfs:Query>
    </wfs:GetFeature>
    """
    xml_obj = xml_obj%(str(from_id), str(to_id))
    
    response = None
    try:
        response = s.post(url="https://www.wozwaardeloket.nl/woz-proxy/wozloket", data=xml_obj)
        print("scraping woz obj from id %s to id %s . "%(str(from_id), str(to_id)), end="\n")
    except Exception as e:
        logging.error(e)
        logging.error("request has met problem. ")
        logging.error("from_id=%s"%str(from_id))
        logging.error("to_id=%s"%str(to_id))
        
    return response.text


def scrape_range_and_save(arg):
    global objects_total, objects_done
    step = 5000
    json_string = scrape_obj_from_id_to_id(arg*step+1, (arg+1)*step)
    parse_json_save_to_sqlite(json_string=json_string)
    objects_done += 1
    print("----------------- Process: {:2.2f}%------------------".format((objects_done)*100/(objects_total)))



def stage1_scrape_all_obj():
    global objects_total, objects_done

    threads = input("how many threads you want to use? (e.g. 100)")

    print("deploying threads, please wait .... ")
    total_steps = range(0, 200000000)
    objects_total = len(total_steps)

    pool = ThreadPool(int(threads))
    pool.map(scrape_range_and_save, total_steps)
    pool.close()
    pool.join()
    objects_total = 0
    objects_done = 0


def scrape_each_property_price(property_id):
    s = requests.Session()
    s.get("https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&")
    
    xml_obj = \
    """
    <wfs:GetFeature
        xmlns:wfs="http://www.opengis.net/wfs" service="WFS" version="1.1.0" xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <wfs:Query typeName="wozloket:woz_woz_object" srsName="EPSG:28992"
            xmlns:WozViewer="http://WozViewer.geonovum.nl"
            xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:Filter
                xmlns:ogc="http://www.opengis.net/ogc">
                <ogc:PropertyIsEqualTo matchCase="true">
                    <ogc:PropertyName>wobj_obj_id</ogc:PropertyName>
                    <ogc:Literal>%s</ogc:Literal>
                </ogc:PropertyIsEqualTo>
            </ogc:Filter>
        </wfs:Query>
    </wfs:GetFeature>
    """
    
    xml_obj = xml_obj%str(property_id)
    
    response = None
    try:
        response = s.post(url="https://www.wozwaardeloket.nl/woz-proxy/wozloket", data=xml_obj)
        print("--------scraping price of property id %s-------"%str(property_id), end="\n")
    except Exception as e:
        logging.error(e)
        logging.error("request has met problem. ")
        logging.error("property_id: %s"%str(property_id))
        
    return response.text




def parse_each_property_price(json_string):
    try:
        json_obj = json.loads(json_string)['features'][0]['properties']
    except Exception as e:
        logging.error(e)
        logging.error("there is some problem with json loading")
    
    price15, price16 = None, None
    try:
        price15 = int(json_obj['wobj_wrd_woz_waarde'])/1000
    except Exception as e:
        price15 = "none"
        
    try:
        price16 = int(json_obj['wobj_huidige_woz_waarde'])/1000
    except Exception as e:
        price16 = "none"
    
    return "{:.3f}".format(price15), "{:.3f}".format(price16)



if __name__ == '__main__':
    init_database()
    stage1_scrape_all_obj()
    print("script finished :) ")