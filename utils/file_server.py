#!/usr/bin/env python3
"""简单文件服务器 - 强制 UTF-8"""

import http.server
import socketserver
import os

PORT = 80
DIR = "/root/.openclaw/workspace"

class UTF8Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        path = self.translate_path(self.path)
        if self.path.startswith('/logs/') or self.path.endswith('.log'):
            self.send_error(403, "Access denied")
            return
        
        if os.path.isdir(path):
            items = sorted(os.listdir(path))
            html = f'''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Files</title>
<style>
body{{font-family:Arial,sans-serif;margin:20px}}
a{{color:#06f;text-decoration:none}}
li{{margin:5px 0}}
</style>
</head>
<body>
<h1>Index of {self.path}</h1>
<hr>
<ul>'''
            for name in items:
                href = os.path.join(self.path, name)
                if os.path.isdir(os.path.join(path, name)):
                    href += '/'
                    name += '/'
                html += f'<li><a href="{href}">{name}</a></li>'
            html += '</ul></body></html>'
            
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', len(html.encode('utf-8')))
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))
            return
        
        try:
            with open(path, 'rb') as f:
                content = f.read()
            
            ext = os.path.splitext(path)[1]
            ctype = {
                '.py': 'text/x-python; charset=utf-8',
                '.md': 'text/markdown; charset=utf-8',
                '.json': 'application/json; charset=utf-8',
                '.txt': 'text/plain; charset=utf-8',
            }.get(ext, 'application/octet-stream')
            
            self.send_response(200)
            self.send_header('Content-Type', ctype)
            self.send_header('Content-Length', len(content))
            self.end_headers()
            self.wfile.write(content)
        except:
            self.send_error(404)
    
    def log_message(*args): pass

os.chdir(DIR)
with socketserver.TCPServer(('', PORT), UTF8Handler) as httpd:
    print(f'Running on http://localhost/')
    httpd.serve_forever()
