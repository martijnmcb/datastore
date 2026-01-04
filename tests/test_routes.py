def test_login_page_renders(client):
    resp = client.get("/auth/login")
    assert resp.status_code == 200
    assert b"Gebruikersnaam" in resp.data


def test_home_requires_login(client):
    resp = client.get("/", follow_redirects=False)
    assert resp.status_code in (301, 302)
    assert "/auth/login" in resp.headers.get("Location", "")


def test_admin_requires_login(client):
    resp = client.get("/beheer/", follow_redirects=False)
    assert resp.status_code in (301, 302)
    assert "/auth/login" in resp.headers.get("Location", "")

