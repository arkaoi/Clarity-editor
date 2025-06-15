async def test_put_and_get(service_client):

    response = await service_client.put('/database/testkey', json={'value': 'value123'})
    assert response.status_code == 200, f"PUT failed: {response.text}"


    response = await service_client.get('/database/testkey')
    assert response.status_code == 200, f"GET failed: {response.text}"
    data = response.json()
    assert data["key"] == "testkey"
    assert data["value"] == "value123"


async def test_delete(service_client):

    response = await service_client.put('/database/deletekey', json={'value': 'to_delete'})
    assert response.status_code == 200, f"PUT for delete failed: {response.text}"


    response = await service_client.delete('/database/deletekey')
    assert response.status_code == 200, f"DELETE failed: {response.text}"
    data = response.json()
    assert data["deleted_key"] == "deletekey"


    response = await service_client.get('/database/deletekey')
    assert response.status_code == 404, f"GET after delete failed: {response.text}"


async def test_missing_key(service_client):

    response = await service_client.get('/database/')
    assert response.status_code == 400, f"GET without key should return 400: {response.text}"


    response = await service_client.put('/database/', json={'value': 'value123'})
    assert response.status_code == 400, f"PUT without key should return 400: {response.text}"


    response = await service_client.delete('/database/')
    assert response.status_code == 400, f"DELETE without key should return 400: {response.text}"