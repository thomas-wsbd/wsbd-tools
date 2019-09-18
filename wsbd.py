"""
This module contains classes that can interact
with the HydroNET API to retrieve and store data.
"""

from os.path import isfile, splitext,join
from os import makedirs

import urllib.parse
import urllib.error
import urllib.request
import logging
import json
import re
import pandas as pd
import geopandas as gpd
import datetime
import numpy as np

from shapely.geometry import Point
from datetime import date

def write_pretty_json(data,filename):

    with open(filename,'w') as file:
        file.write(json.dumps(data,
                              indent = 4,
                              sort_keys = False))

class APIClient():
    def execute_request(self, full_url):

        data = None
        try:
            request = urllib.request.Request(full_url)
            with urllib.request.urlopen(request) as response:
                data = response.read()
        except urllib.error.HTTPError as err:
            logging.error('The server couldn\'t fulfill the request.')
        except urllib.error.URLError as err:
            logging.error(err)

        return data

class Hydro(APIClient):

    url = 'http://hydro.dommel.nl/'

    requestname = [
        'GetDwsLog',            # Return the dws log table from the dws database
        'GetRequestLog', 	    # Return the request log table from the dws database
        'GetUpdateStatusLog',   # Return the update status log table from the dws database
        'GetStatus',            # Return the status table from the dws database
        'GetDataSources',       # Return the DataSources configured in the DWS
        'GetLocations',         # Return the Locations configured for a specific theme in the DWS
        'GetGeoLocations', 	    # Return the Locations based on some EPSG configured in the DWS
        'GetParameterGroups', 	# Return the parameter groups configured in the DWS
        'GetParameters',	    # Return the parameters based on a theme configured in the DWS
        'GetThmeCategories', 	# Return the theme categories configured in the DWS
        'GetThemes', 	        # Return the themes configured in the DWS
        'GetTimeseries', 	    # Return the timeseries configured in the DWS
        'GetData', 	            # Get data from the source system and return it
        'GetDataCalc']          # Get data from the source system, perform an operation on the data and return it

    formats = [
        'xml',
        'html',
        'json',
        'jsonp',
        'jsont',
        'csv']

    def create_request(self,data,parameters=None):

        try:
            if data['request'] not in self.requestname:
                raise RuntimeError(
                    'Invalid request name: {}.\n'.format(data['request']) +
                    'Requestname can be: {}'.format(self.requestname))
            if data['format'] not in self.formats:
                raise RuntimeError(
                    'Invalid format: {}.\n'.format(data['format']) +
                    'Accepted formats are: {}'.format(self.formats))
        except KeyError:
            raise RuntimeError('"request" and "format" are mandatory fields in the request.')

        data_values = urllib.parse.urlencode(data)
        full_url = self.url + 'dws?' + data_values

        if parameters is not None:
            parameters_values = urllib.parse.urlencode(parameters)
            full_url += '&' + parameters_values

        logging.debug('Full request url: %s', full_url)

        return full_url
    
    def request(self,cfg,parameters):
        logging.debug('Retrieving data in the %s format',cfg['format'])

        try:

            request = self.create_request(cfg,parameters)
            data = self.execute_request(request).decode('utf-8-sig')

            if data is None or data == "":
                raise RuntimeError('No response from the API.')

            if re.search("Input string was not in a correct format.", data):
                raise RuntimeError('The input string was not in a correct format.')

            if cfg['format'] == 'json':
                output = json.loads(data)
                if output['Data'] == []:
                    raise RuntimeError('The Data field is empty!')
                if output['ErrorMessage'] != "":
                    raise RuntimeError('Error executing the query.\n'+data['ErrorMessage'])

            return output

        except BaseException as err:
            logging.error(err)

    def download(self,cfg,parameters,filename):

        logging.debug('Downloading data in the %s format',cfg['format'])

        try:

            request = self.create_request(cfg,parameters)
            data = self.execute_request(request).decode('utf-8-sig')

            if data is None or data == "":
                raise RuntimeError('No response from the API.')

            if re.search("Input string was not in a correct format.", data):
                raise RuntimeError('The input string was not in a correct format.')

            if isfile(filename):
                logging.warning('Overwriting: %s',filename)

            if cfg['format'] == 'json':
                output = json.loads(data)
                if output['Data'] == []:
                    raise RuntimeError('The Data field is empty!')
                if output['ErrorMessage'] != "":
                    raise RuntimeError('Error executing the query.\n'+data['ErrorMessage'])

                logging.debug('Saving data to file: %s',filename)
                write_pretty_json(output,filename)

        except BaseException as err:
            logging.error(err)

    def download_geolocations(self,themeid,filename):

        extension = splitext(filename)[1][1:] # without the dot.

        self.download(
            cfg = {
                'request' : 'GetGeoLocations',
                'format'  :  extension,
                },
            parameters = {
                'theme'   :  themeid
            },
            filename=filename)
        
    def return_geolocations(self,themeid,codefilter=None,namefilter=None):
        """Returns GeoDataFrame with locations for given themeid
        
        Parameters
        ----------
        themeid: int
            themeid as integer, 
        codefilter: str
            codefilter can be used to select certain stations from WISKI, for example,
            TDB selects all discharge stations, other codefilters for WISKI include
            WTH_BOV, WTH_BEN, WTH, KKL, PBF etc.
        namefilter: str
            namefilter can be used to select certain stations, is executed after codefilter
        
        Returns
        -------
        gdf: GeoDataFrame
            GeoDataFrame containing geolocation information.
        """
        
        r = self.request(
                    cfg = {
                        'request' : 'GetGeoLocations',
                        'format'  : 'json'
                    },
                    parameters = {
                        'theme'   : themeid
                    })
        geolocations = pd.DataFrame(r['Data'])
        geolocations = pd.concat([geolocations,pd.DataFrame([x[0] for x in geolocations['Projections']])], axis=1)
        geometry = [Point(x,y) for x,y in zip(geolocations.X,geolocations.Y)]

        crs = {'init': 'epsg:'+geolocations.EPSG[0]}
        gdf = gpd.GeoDataFrame(geolocations,geometry=geometry,crs=crs)
        if codefilter:
            gdf = gdf[gdf.Code.str.contains(codefilter)]
        if namefilter:
            gdf = gdf[gdf.Name.str.contains(namefilter)]
        return gdf

    def download_themes(self,filename):

        extension = splitext(filename)[1][1:] # without the dot.

        self.download(
            cfg = {
                'request' : 'GetThemes',
                'format'  :  extension,
                },
            parameters = None,
            filename=filename)
    
    def return_themes(self):
        """Returns DataFrame with defined themes
        
        Parameters
        ----------
        
        
        Returns
        -------
        themes: DataFrame
            DataFrame containing theme information.
        """
        
        r = self.request(
                cfg = {
                    'request' : 'GetThemes',
                    'format'  :  'json',
                    },
                parameters = None)
        themes = pd.DataFrame(r['Data'])
        return themes
    
    def download_locations_data(self,themeid,filename):

        extension = splitext(filename)[1][1:]
        self.download(
            cfg = {
                'request' : 'GetLocations',
                'format'  :  extension,
                },
            parameters = {
                'theme'   : themeid
                },
            filename=filename)
        
    def return_locations_data(self,themeid):
        """Returns DataFrame with locations for given themeid
        
        Parameters
        ----------
        themeid: int
            themeid as integer, see self.return_themes() for themes
        
        Returns
        -------
        locations: DataFrame
            DataFrame containing location information for given themeid.
        """
        
        r = self.request(
                cfg = {
                    'request' : 'GetLocations',
                    'format'  : 'json',
                    },
                parameters = {
                    'theme'   : themeid
                    })
        locations = pd.DataFrame(r['Data'])
        return locations

    def download_parameters(self,themeid,filename):

        extension = splitext(filename)[1][1:] # without the dot.
        self.download(
            cfg = {
                'request' : 'GetParameters',
                'format'  :  extension,
                },
            parameters = {
                'theme'   : themeid,
            },
            filename=filename)
        
    def return_parameters(self,themeid):
        """Returns DataFrame with parameters for given themeid
        
        Parameters
        ----------
        themeid: int
            themeid as integer, see self.return_themes() for themes
        
        Returns
        -------
        parameters: DataFrame
            DataFrame containing parameter information for given themeid.
        """
        
        r = self.request(
                cfg = {
                    'request' : 'GetParameters',
                    'format'  :  'json',
                    },
                parameters = {
                    'theme'   : themeid,
                })
        parameters = pd.DataFrame(r['Data'])
        return parameters
    
    def return_timeseries(self,themeid):
        """Returns DataFrame with timeseries for given themeid
        
        Parameters
        ----------
        themeid: int
            themeid as integer, see self.return_themes() for themes
        
        Returns
        -------
        timeseries: DataFrame
            DataFrame containing timeseries information for given themeid.
        """
        
        r = self.request(
                cfg = {
                    'request' : 'GetTimeseries',
                    'format'  :  'json',
                    },
                parameters = {
                    'theme'   : themeid,
                })
        timeseries = pd.DataFrame(r['Data'])
        return timeseries
        

    def download_data(self,themes,parameters,locations,filename,startdate=date(date.today().year,1,1),enddate=date.today()):

        extension = splitext(filename)[1][1:] # without the dot.

        loc  = ','.join([str(location['LocationID'])   for location  in locations  ])
        pars = ','.join([str(parameter['ParameterID']) for parameter in parameters ])
        thms = ','.join([str(theme['ThemeId']) for theme in themes ])# TODO if parameter ['visible'] and ['enabled']?
        strd = startdate.strftime('%Y%m%d0000')
        endd = enddate.strftime('%Y%m%d0000')

        if loc == '' or pars == '' or thms == '':
            logging.warning('Empty locations, parameters or theme array. Skipping this query.' )
            return

        self.download(
            cfg = {
                'request' : 'GetData',
                'format'  :  extension,
            },
            parameters = {
                'theme'      : thms,
                'parameters' : pars,
                'locations'  : loc,
                'startdate'  : strd,
                'enddate'    : endd
            },
            filename=filename)
        
    def return_data(self,theme,parameter,location,timeserie=39,startdate=None,enddate=None,values=True):
        """Returns DataFrame with measurements for given theme, parameter and location
        
        Parameters
        ----------
        themeid: int, list
            themeid as integer, see self.return_themes() for themes
        parameter: int, str, list
            parameter in int is HydroNET parameter ID, parameter in str is fullname parametername, see 
            self.return_parameters(theme_id) for possible parameters given theme
        location: int, str, list
            parameter in int is HydroNET location ID, locations in str is name given in WISKI, see
            self.return_locations(theme_id) for possible locations given theme
        timeserie: int, list
            timeseries id, for WISKI 38: ContinueMeting.P, 39: Dag.Gem, 40: Maand.Gem. 41: Jaar.Gem, see
            self.return_timeseries(theme_id) for possible timeseries given theme
        startdate: str
            startdate in string format, for example '20190101', default is first day of this year
        enddate: str
            enddate in string format, for example '20190101', default is today
        values: bool
            if values is True DataFrame containing only series is return, if values False, DataFrame with
            series and metadata is returned
        
        Returns
        -------
        data: DataFrame
            DataFrame containing measurements for given filters.
        """
        if startdate:
            strd = pd.to_datetime(startdate).strftime('%Y%m%d0000')
        else:
            strd = date(date.today().year,1,1).strftime('%Y%m%d0000')
        if enddate:
            endd = pd.to_datetime(enddate).strftime('%Y%m%d0000')
        else:
            endd = date.today().strftime('%Y%m%d0000')
                      
        if location == '' or parameter == '' or theme == '':
            logging.warning('Empty location, parameter or theme. Skipping this query.' )
            return
        
        parameters = {
                    'theme'      : theme,
                    'startdate'  : strd,
                    'enddate'    : endd,
                    'timeserie'  : timeserie
                }
        if isinstance(parameter, np.integer) or isinstance(parameter, int):
            parameters.update({'parameter' :  parameter})
        if isinstance(parameter, str):
            parameters.update({'parcode'   :  parameter})
        if isinstance(parameter, list):
            parameters.update({'parcode'   :  ','.join(parameter)})
        if isinstance(location, np.integer) or isinstance(location, int):
            parameters.update({'loc'       :  location})
        if isinstance(location, str):
            parameters.update({'loccode'   :  location})
        r = self.request(
                cfg = {
                    'request' : 'GetData',
                    'format'  :  'json',
                },
                parameters = parameters)
        data_raw = pd.DataFrame(r['Data'])
        
        if values:
            data = pd.DataFrame(data_raw['Values'][0])
            data.set_index(pd.to_datetime(data['Date']), inplace=True)
            data.drop(columns=['Date','QC'], inplace=True)
            data.columns = [data_raw['Name'][0]]
        else:
            data = data_raw
            
        return data