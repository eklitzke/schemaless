import os
import optparse
import datetime

import tornado.web
import tornado.ioloop
import tornado.httpserver

import schemaless
from schemaless import c
from schemaless import orm

dirname = os.path.dirname(__file__)

##############
# ORM Things
##############

datastore = schemaless.DataStore(mysql_shards=['localhost:3306'], user='test', password='test', database='test')
session = orm.Session(datastore)
Base = orm.make_base(session, tags_file=os.path.join(dirname, 'tags.yaml'))

class Post(Base):
    _columns = [
        orm.VARCHAR('title', 255, nullable=False),
        orm.TEXT('content', nullable=False),
        orm.DATETIME('time_created', default=datetime.datetime.now)
        ]

    _indexes = [['time_created']]

    @classmethod
    def new_post(cls, title, content):
        return cls(post_id=schemaless.guid(), title=title, content=content).save()

    @property
    def comments(self):
        """Get all the comments for this post, ordered by time created."""
        if not hasattr(self, '_comments'):
            comments = Comment.query(c.post_id == self.id)
            self._comments = sorted(comments, key=lambda c: c.time_created)
        return self._comments

class Comment(Base):
    _columns = [
        orm.GUID('post_id', 32, nullable=False),
        orm.VARCHAR('author', 255),
        orm.TEXT('content', nullable=False),
        orm.DATETIME('time_created', default=datetime.datetime.now)
        ]

    _indexes = [['comment_id']]

    @classmethod
    def reply(cls, post_id, author, content):
        return cls(comment_id=schemaless.guid(), post_id=post_id, author=author, content=content).save()

##############
# Tornado Things
##############

class MainHandler(tornado.web.RequestHandler):

    def get(self):
        posts = sorted(Post.all(), key=lambda x: x.time_created, reverse=True)
        self.render('main.html', title='Blog', posts=posts)

class PostHandler(tornado.web.RequestHandler):

    def get(self):
        self.render('post.html', title='New Post')

    def post(self):
        title = self.get_argument('title')
        content = self.get_argument('content')
        Post.new_post(title, content)
        self.redirect('/')

class CommentHandler(tornado.web.RequestHandler):

    def post(self):
        post_id = self.get_argument('post_id')
        author = self.get_argument('author')
        content = self.get_argument('content')
        Comment.reply(post_id, author, content)
        self.redirect('/')

settings = {
    'static_path': os.path.join(dirname, 'static'),
    'template_path': os.path.join(dirname, 'templates'),
    'cookie_secret': '61oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=',
    'xsrf_cookies': True,
}

application = tornado.web.Application([
        ('/', MainHandler),
        ('/post', PostHandler),
        ('/comment', CommentHandler)], **settings)

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-p', '--port', type='int', default='8888', help='which port to listen on')
    parser.add_option('-c', '--clear', action='store_true', default=False, help='clear all tables when starting')
    opts, args = parser.parse_args()

    if opts.clear:
        tables = set()
        for d in datastore.connection.query('SHOW TABLES'):
            tables |= set(d.values())
        for t in tables:
            datastore.connection.execute('DELETE FROM %s' % t)

    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(opts.port)
    print 'blog waiting at http://localhost:%d' % opts.port
    tornado.ioloop.IOLoop.instance().start()
