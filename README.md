# Spider_wozwaardeloket
This is a scrapper for site: www.wozwaardeloket.nl which is a real estate information website.

## target data with some examples
| search_locatie | street | housenumber | housenumber_ext | postcode | plaatsnaam | identificatie | price_2016 | price_2015 | bouwjaar | gebruiksdoel | oppervlakte | pdf_link |
|---------------|---------|-------------|-----------------|----------|------------|---------------|------------|------------|----------|--------------|-------------|----------|
| 7523 VT | Jekerstraat | 102 | none | 7523VS | Enschede | 015300024745 | 115.000 | 115.000 | 1969 | woonfunctie | 92 | https://xxxx |  

## This is a good example of massive-level spider
* target website use cookies to protect itself from scrapping.
* we have tons of postcodes that need to be scrapped, we need to increase the speed to at least 200 pages/seconds.
* this script is built on top of scrapy framwork and utilized docker container technology to speed up.


## how the website works
> this is a website's entry point, before we can initiate any request, we need to send a get request to the following link
[https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&](https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&)

* the website requires you to have a list of post code in Netherlands, here we have a copy of that.
[all post codes of netherlands (csv file)](https://raw.githubusercontent.com/XetRAHF/Github_Markdown-Files/192628ff1cb7a7beadf5cedc0b4bacff724e9020/postcode_list.csv)

* here we choose a postcode for demostration `7523VT`, when you type in the code in that textfield, the website will start a new request then give you the following result
![spider_stage_1](https://raw.githubusercontent.com/XetRAHF/Github_Markdown-Files/d4e36acc933b87455d0e2c7cb963249ca7dafa4b/postcode_stage_1.PNG)

* click on `Jekerstraat 102` which is on the right side, then the website start a new request with following response
![spider_stage_2](https://raw.githubusercontent.com/XetRAHF/Github_Markdown-Files/d4e36acc933b87455d0e2c7cb963249ca7dafa4b/postcode_stage_2.PNG)

## What is happening in the background
* when we initiated following request to the website, it will set cookies on the browser, which is used for the website to detect web-bot
    * [https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&](https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&)

* when we input a postcode, says `7523VT` to the website, the site will search the postcode on it's database. 
    * this is the corresponding ajax request (GET): 
        * [https://www.wozwaardeloket.nl/api/geocoder/v2/suggest?query=7523VT](https://www.wozwaardeloket.nl/api/geocoder/v2/suggest?query=7523VT)
    * this ajax gives us following response: 
        * ![stage1_ajax.PNG](https://raw.githubusercontent.com/XetRAHF/Github_Markdown-Files/c2edf2e672873d2255a753c1d4573542a5ca6ae0/stage1_ajax.PNG)
    
    * another ajax request (GET) will start immediately after this request is completed
        * [https://www.wozwaardeloket.nl/api/geocoder/v2/lookup?id=pcd-55b2e73c50b5f35b9adcadf40768de91](https://www.wozwaardeloket.nl/api/geocoder/v2/lookup?id=pcd-55b2e73c50b5f35b9adcadf40768de91)
        * gives us following response:
            * ![stage2_ajax.PNG](https://raw.githubusercontent.com/XetRAHF/Github_Markdown-Files/c2edf2e672873d2255a753c1d4573542a5ca6ae0/stage2_ajax.PNG)

    
    * another ajax request (POST) will start immediately after this request is completed
        * ![https://www.wozwaardeloket.nl/woz-proxy/wozloket](https://www.wozwaardeloket.nl/woz-proxy/wozloket)
        * POST headers
            * ![stage3_ajax_headers.PNG](https://raw.githubusercontent.com/XetRAHF/Github_Markdown-Files/c2edf2e672873d2255a753c1d4573542a5ca6ae0/stage3_ajax_headers.PNG)
        * POST body
            ```
            <wfs:GetFeature
                xmlns:wfs="http://www.opengis.net/wfs" service="WFS" version="1.1.0" xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <wfs:Query typeName="wozloket:woz_woz_object" srsName="EPSG:28992"
                    xmlns:WozViewer="http://WozViewer.geonovum.nl"
                    xmlns:ogc="http://www.opengis.net/ogc">
                    <ogc:Filter
                        xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:And>
                            <ogc:Contains>
                                <ogc:PropertyName>wobj_geometrie</ogc:PropertyName>
                                <gml:Point
                                    xmlns:gml="http://www.opengis.net/gml">
                                    <gml:pos>257727.8 473442.88</gml:pos>
                                </gml:Point>
                            </ogc:Contains>
                            <ogc:BBOX>
                                <ogc:PropertyName>wobj_geometrie</ogc:PropertyName>
                                <gml:Envelope
                                    xmlns:gml="http://www.opengis.net/gml">
                                    <gml:lowerCorner>257726.8 473441.88</gml:lowerCorner>
                                    <gml:upperCorner>257728.8 473443.88</gml:upperCorner>
                                </gml:Envelope>
                            </ogc:BBOX>
                        </ogc:And>
                    </ogc:Filter>
                </wfs:Query>
            </wfs:GetFeature>
            ```
        * gives us following response
            * ![stage3_ajax_response.PNG](https://raw.githubusercontent.com/XetRAHF/Github_Markdown-Files/de22ad68815b95130473050dc139d2fab0afd0d2/stage3_ajax_response.PNG)

* after we click `Jekerstraat 102` which is on the right side, the browser will start some new requests, we only need to know the critical request that brings us the price of property
    * this is the url of the request, same as the previous request.
        [https://www.wozwaardeloket.nl/woz-proxy/wozloket](https://www.wozwaardeloket.nl/woz-proxy/wozloket) 
    * with the same header as the previous request.
    * but with different POST body
        ```
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
                        <ogc:Literal>015300024745</ogc:Literal>
                    </ogc:PropertyIsEqualTo>
                </ogc:Filter>
            </wfs:Query>
        </wfs:GetFeature>
        ```
    * gives us following response
        * ![stage4_response.PNG](https://raw.githubusercontent.com/XetRAHF/Github_Markdown-Files/1b7ed405f8a26c1fd365bd3b3bf44fc7d715f527/stage4_response.PNG)


## how we can scrape the site

















 