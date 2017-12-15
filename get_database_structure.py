from __future__ import absolute_import, division, print_function, \
    with_statement
import requests
from peewee import *
import json
import numpy
import logging
from multiprocessing.dummy import Pool as ThreadPool
import threading


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
    price_2017 = CharField(null=True)

    bouwjaar = CharField(null=True)
    gebruiksdoel = CharField(null=True)
    oppervlakte = CharField(null=True)

class stage1Scrapper():
    def __init__(self):
        self.threads = int()

        self.init_database()
        self.ask_user_questions()

    def run(self):
        self.scrape_all_obj(self.threads)

    def ask_user_questions(self):
        self.threads = input("how many threads you want to use? (e.g. 100)")
        print("deploying threads, please wait .... ")

    def scrape_all_obj(self, threads):
        total_requests = range(0, 200000000)

        chunks = [total_requests[x:x + 1000] for x in range(0, len(total_requests), 1000)]
        for each_chunk in chunks:
            pool = ThreadPool(int(threads))
            pool.map(self.scrape_range_and_save, each_chunk)
            pool.close()
            pool.join()

    def scrape_obj_from_id_to_id(self, f=None, t=None):
        if f is None:
            raise Exception("From id is None, check the script please. ")
        if t is None:
            raise Exception("To id is None, check the script please. ")

        # format from_id and to_id
        from_id = '%012d' % f
        to_id = '%012d' % t

        # initiate a request object
        s = requests.Session()
        try:
            s.get("https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&")
        except Exception as e:
            logging.error(e)
            logging.error("Please fix your network connection status immediately")
            logging.error("the script will wait for you about 10 seconds :< ")

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
        xml_obj = xml_obj % (str(from_id), str(to_id))

        response = None
        try:
            response = s.post(url="https://www.wozwaardeloket.nl/woz-proxy/wozloket", data=xml_obj)
            print("scraping woz obj from id %s to id %s . " % (str(from_id), str(to_id)), end="\n")
        except Exception as e:
            logging.error(e)
            logging.error("request has met problem. ")
            logging.error("from_id=%s" % str(from_id))
            logging.error("to_id=%s" % str(to_id))

        text_response = None
        try:
            text_response = response.text
        except:
            text_response = None

        return text_response

    def parse_json_save_to_sqlite(self, json_string):
        json_obj = None
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

        obj_features = []
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

            pr_2015, pr_2016, pr_2017 = \
                parse_each_property_price(scrape_each_property_price(building.identificatie))

            if pr_2015 is not None:
                building.price_2015 = pr_2015

            if pr_2016 is not None:
                building.price_2016 = pr_2016

            if pr_2017 is not None:
                building.price_2017 = pr_2017

            try:
                building.save()
            except Exception as e:
                logging.error(e)
                logging.error("Got a duplicate record")

    def scrape_range_and_save(self, arg):
        step = 5000
        json_string = self.scrape_obj_from_id_to_id(arg * step + 1, ((arg + 1) * step) + 2)
        self.parse_json_save_to_sqlite(json_string=json_string)

    def init_database(self):
        db = SqliteDatabase("netherland_properties.db")
        db.connect()
        db.create_tables([PropertyModel])
        db.close()



def main():
    scrapper = stage1Scrapper()
    scrapper.run()

if __name__ == '__main__':
    main()