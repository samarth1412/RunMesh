import net from 'node:net'

for (const port of [3000, 8080, 8180, 9000]) {
  net.createServer((client) => {
    const upstream = net.connect(port, 'host.docker.internal')
    client.on('error', () => upstream.destroy())
    upstream.on('error', () => client.destroy())
    client.pipe(upstream)
    upstream.pipe(client)
  }).listen(port, '127.0.0.1')
}
