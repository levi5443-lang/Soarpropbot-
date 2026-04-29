import os
from aiohttp import web

async def serve_index(request):
    return web.FileResponse('./index.html')

async def serve_privacy(request):
    return web.FileResponse('./privacy.html')

async def serve_terms(request):
    return web.FileResponse('./terms.html')

app = web.Application()
app.router.add_get('/', serve_index)
app.router.add_get('/privacy', serve_privacy)
app.router.add_get('/privacy.html', serve_privacy)
app.router.add_get('/terms', serve_terms)
app.router.add_get('/terms.html', serve_terms)
app.router.add_static('/static', path='./static', name='static')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    web.run_app(app, host='0.0.0.0', port=port)
