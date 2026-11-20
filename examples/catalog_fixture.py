"""Exercise the real catalog client over owned in-memory HTTP transport."""
import json
import uuid

import httpx
from catalog_client import CatalogClient
from catalog_client.models.dataset import DatasetRequest, DatasetResponse
from catalog_client.models.asset import DataAssetResponse


class CatalogFixture:
    def __init__(self):
        self.records={}
        self.events=[]
        self.http=httpx.Client(base_url='https://catalog.invalid/api/',transport=httpx.MockTransport(self.handle))
        self.client=CatalogClient(base_url='https://catalog.invalid',api_token='local-fixture-only')
        self.client._http.close()
        self.client._http=self.http
        for part in [self.client.datasets,self.client.lineages,self.client.collections]:
            part._http=self.http

    def handle(self,request):
        path=request.url.path
        params=dict(request.url.params)
        if request.url.host!='catalog.invalid':raise AssertionError('External catalog request')
        self.events.append({'method':request.method,'path':path,'params':params})
        if request.method=='GET' and path.rstrip('/')=='/api/datasets':
            found=[value for value in self.records.values()
                   if all(str(value.get(key))==params[key] for key in ['canonical_id','version','project'] if key in params)]
            return httpx.Response(200,json={'results':found,'total':len(found),'offset':0,'limit':int(params.get('limit',100))})
        if request.method=='GET' and path.startswith('/api/datasets/'):
            value=self.records.get(path.rsplit('/',1)[-1])
            return httpx.Response(404,json={'detail':'missing'}) if value is None else httpx.Response(200,json=value)
        if request.method=='POST' and path.rstrip('/')=='/api/datasets':
            value=DatasetRequest.model_validate_json(request.content).model_dump(mode='json')
            key=str(uuid.uuid4())
            now='2026-11-19T00:00:00Z'
            value.update(id=key,tombstoned=False,created_at=now,last_modified_at=now,record_version=1)
            for asset in value['locations']:
                asset.update(id=str(uuid.uuid4()),dataset_id=key,tombstoned=False,created_at=now,last_modified_at=now)
                DataAssetResponse.model_validate(asset)
            DatasetResponse.model_validate(value)
            self.records[key]=value
            return httpx.Response(201,json=value)
        raise AssertionError('Unexpected catalog operation: '+request.method+' '+path)

    def save(self,path):
        with open(path,'x') as stream:json.dump({'records':list(self.records.values()),'http_events':self.events,'transport':'httpx.MockTransport; no deployed API or real network'},stream,indent=2)
