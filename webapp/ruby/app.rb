require 'sinatra/base'
require 'mysql2'
require 'mysql2-cs-bind'
require 'erubis'
require 'rack/lineprof'
require 'rack/session/redis'
require 'redis'

module Ishocon1
  class AuthenticationError < StandardError; end
  class PermissionDenied < StandardError; end
end

class Ishocon1::WebApp < Sinatra::Base
  enable :logging
  use Rack::Session::Redis, redis_server: 'redis://localhost:6379/', expires_in: 3600
  use Rack::Lineprof
  set :erb, escape_html: true
  set :public_folder, File.expand_path('../public', __FILE__)
  set :protection, true

  PRD_MAX_ID = 10000

  USER_ID_KEY_PREFIX = 'user_id_'
  USER_BOUGHT_PREFIX = 'user_bought_'

  LATEST_COMMENT_NAME_PREFIX = 'lastest_comment_name_'
  LATEST_COMMENT_CONTENT_PREFIX = 'lastest_comment_content_'
  
  PRODUCT_PREFIX = 'product_'

  helpers do
    def config
      @config ||= {
        db: {
          host: ENV['ISHOCON1_DB_HOST'] || 'localhost',
          port: ENV['ISHOCON1_DB_PORT'] && ENV['ISHOCON1_DB_PORT'].to_i,
          username: ENV['ISHOCON1_DB_USER'] || 'ishocon',
          password: ENV['ISHOCON1_DB_PASSWORD'] || 'ishocon',
          database: ENV['ISHOCON1_DB_NAME'] || 'ishocon1'
        }
      }
    end

    def db
      return Thread.current[:ishocon1_db] if Thread.current[:ishocon1_db]
      client = Mysql2::Client.new(
        host: config[:db][:host],
        port: config[:db][:port],
        username: config[:db][:username],
        password: config[:db][:password],
        database: config[:db][:database],
        reconnect: true
      )
      client.query_options.merge!(symbolize_keys: true)
      Thread.current[:ishocon1_db] = client
      client
    end

    def redis
      return Thread.current[:redis] if Thread.current[:redis]
      redis = Redis.new(path: "/var/run/redis/redis-server.sock")
      Thread.current[:redis] = redis
      redis
    end

    def time_now_db
      Time.now - 9 * 60 * 60
    end

    def authenticate(email, password)
      user = db.xquery('SELECT * FROM users WHERE email = ?', email).first
      fail Ishocon1::AuthenticationError unless user.nil? == false && user[:password] == password
      session[:user_id] = user[:id]
      session[:user_name] = user[:name]
    end

    def authenticated!
      fail Ishocon1::PermissionDenied unless current_user
    end

    def current_user
      redis.exists?("#{USER_ID_KEY_PREFIX}#{session[:user_id]}") ? {id: session[:user_id], name: session[:user_name]} : nil
    end

    def buy_product(product_id, user_id)
      redis.hincrby("#{USER_BOUGHT_PREFIX}#{user_id}", product_id, 1)
      db.xquery('INSERT INTO histories (product_id, user_id, created_at) VALUES (?, ?, ?)', \
            product_id, user_id, time_now_db)
    end

    def already_bought?(product_id)
      return false unless current_user
      redis.hexists("#{USER_BOUGHT_PREFIX}#{current_user[:id]}", product_id)
    end

    def create_comment(product_id, user_id, user_name, content)
      content = content.size > 25 ? content[0..24]+'…' : content

      redis.watch(
        "#{LATEST_COMMENT_NAME_PREFIX}#{product_id}",
        "#{LATEST_COMMENT_CONTENT_PREFIX}#{product_id}"
      ) do
        exec_trim = redis.llen("#{LATEST_COMMENT_NAME_PREFIX}#{product_id}") >= 5
        result = redis.multi do |multi|
            redis.hincrby("#{PRODUCT_PREFIX}#{product_id}", 'comments_count', 1)
            redis.lpush("#{LATEST_COMMENT_NAME_PREFIX}#{product_id}", user_name)
            redis.lpush("#{LATEST_COMMENT_CONTENT_PREFIX}#{product_id}", content)
            if exec_trim then
              redis.ltrim("#{LATEST_COMMENT_NAME_PREFIX}#{product_id}", 0, 4)
              redis.ltrim("#{LATEST_COMMENT_CONTENT_PREFIX}#{product_id}", 0, 4)
            end
        end
      end
    end

    def product_str_to_sym(product)
      new_hash = {}
      product.keys.each do |key|
        new_hash[key.to_sym] = product[key]
      end
      if new_hash[:created_at].is_a?(String) then
        new_hash[:created_at] = Time.parse(new_hash[:created_at])
      end
      new_hash
    end

		# https://gist.github.com/bryanthompson/277560
		# 部分キャッシュここから
		def cache(name, &block)
			if cache = read_fragment(name)
				@_out_buf << cache
			else
				pos = @_out_buf.length
				tmp = block.call
				write_fragment(name, tmp[pos..-1])
			end        
		end

		def read_fragment(name)
			cache_file = "/tmp/sinatra_cache/#{name}.cache"
			now = Time.now
			if File.file?(cache_file)
				return File.read(cache_file)
			end
			false
		end

		def write_fragment(name, buf)
			cache_file = "/tmp/sinatra_cache/#{name}.cache"
			f = File.new(cache_file, "w+")
			f.write(buf)
			f.close
			buf
		end

		#独自拡張
		def remove_fragment(name)
			cache_file = "/tmp/sinatra_cache/#{name}.cache"
		end
		# 部分キャッシュここまで
  end

  error Ishocon1::AuthenticationError do
    session[:user_id] = nil
    halt 401, erb(:login, layout: false, locals: { message: 'ログインに失敗しました' })
  end

  error Ishocon1::PermissionDenied do
    halt 403, erb(:login, layout: false, locals: { message: '先にログインをしてください' })
  end

  get '/login' do
    session.clear
    erb :login, layout: false, locals: { message: 'ECサイトで爆買いしよう！！！！' }
  end

  post '/login' do
    authenticate(params['email'], params['password'])
    redirect '/'
  end

  get '/logout' do
    session.clear
    redirect '/login'
  end

  get '/' do
    cache_control :public
    page = params[:page].to_i || 0
    limit = 50

    
    product_ids = ((PRD_MAX_ID - ((page + 1) * limit) + 1)..(PRD_MAX_ID - (page * limit))).to_a.reverse

    results = redis.pipelined do
      product_ids.map do |id|
          redis.mapped_hmget(
            "#{PRODUCT_PREFIX}#{id}",
            'id',
            'name',
            'short_description',
            'image_path',
            'price',
            'created_at',
            'comments_count'
          )
      end
    end

    products = results.map do |result|
      new_hash = product_str_to_sym(result)

      multi_result = redis.multi do |multi|
        multi.lrange("#{LATEST_COMMENT_NAME_PREFIX}#{new_hash[:id]}", 0, -1)
        multi.lrange("#{LATEST_COMMENT_CONTENT_PREFIX}#{new_hash[:id]}", 0, -1)
      end
      comment_names = multi_result[0]
      comment_contents = multi_result[1]
      
      new_hash[:comments] = []
      comment_names.zip(comment_contents) do |name, content|
        new_hash[:comments].push({name: name, content: content})
      end

      new_hash
    end

    erb :index, locals: { products: products }
  end

  get '/users/:user_id' do
    cache_control :public
    history_query =<<SQL
SELECT h.product_id, h.created_at
FROM histories as h
WHERE h.user_id = ?
ORDER BY h.id DESC
SQL
    history = db.xquery(history_query, params[:user_id])

    total_pay = 0
    results = redis.pipelined do
      history.each_with_index do |h,i|
        redis.mapped_hmget(
          "#{PRODUCT_PREFIX}#{h[:product_id]}",
          'id',
          'name',
          'short_description',
          'image_path',
          'price',
          'comments_count'
        )
        break if i >= 30
      end
    end
    products = results.map.with_index do |result, index|
      product = product_str_to_sym(result)
      product
    end
    products.zip(history) do |product, h|
      product[:created_at] = h[:created_at]
    end

    paid_history = redis.pipelined do
      history.each do |h|
        redis.hget("#{PRODUCT_PREFIX}#{h[:product_id]}", 'price')
      end
    end
    total_pay = paid_history.map(&:to_i).inject(:+)

    user = db.xquery('SELECT * FROM users WHERE id = ?', params[:user_id]).first
    erb :mypage, locals: { products: products, user: user, total_pay: total_pay }
  end

  get '/products/:product_id' do
    cache_control :public
    product = db.xquery('SELECT * FROM products WHERE id = ?', params[:product_id]).first
    erb :product, locals: { product: product }
  end

  post '/products/buy/:product_id' do
    authenticated!
    buy_product(params[:product_id], current_user[:id])
    redirect "/users/#{current_user[:id]}"
  end

  post '/comments/:product_id' do
    authenticated!
    create_comment(params[:product_id], current_user[:id], current_user[:name], params[:content])
    redirect "/users/#{current_user[:id]}"
  end

  get '/initialize' do

    db.query('DELETE FROM users WHERE id > 5000')
    db.query('DELETE FROM products WHERE id > 10000')
    db.query('DELETE FROM comments WHERE id > 200000')
    db.query('DELETE FROM histories WHERE id > 500000')


    Dir.foreach('/tmp/sinatra_cache') do |f|
      fn = File.join('/tmp/sinatra_cache', f)
      File.delete(fn) if f != '.' && f != '..'
    end

    redis.flushall

    users = db.query('SELECT id, email FROM users')
    users.each do |user|
      redis.set("#{USER_ID_KEY_PREFIX}#{user[:id]}", user[:email])
    end

    bought_query =<<SQL
SELECT
  user_id,
  product_id,
  count(*) as count
FROM histories
GROUP BY 1,2
HAVING count > 0
SQL
    bought_count = db.xquery(bought_query)
    bought_count.each do |bought|
      redis.hset(
        "#{USER_BOUGHT_PREFIX}#{bought[:user_id]}",
        bought[:product_id],
        bought[:count]
      )
    end

    products_query = <<SQL
SELECT
  p.id,
  p.name,
  p.description,
  p.image_path,
  p.price,
  p.created_at,
  COUNT(c.id) AS comments_count
FROM
  products AS p
JOIN comments AS c
  ON c.product_id = p.id
GROUP BY p.id
SQL
    products = db.xquery(products_query)
    redis.multi do |multi|
      products.each do |product|
        multi.hset(
          "#{PRODUCT_PREFIX}#{product[:id]}",
          "id",
          product[:id],
          "name",
          product[:name],
          "description",
          product[:description],
          "short_description",
          product[:description][0..69]+'…',
          "image_path",
          product[:image_path],
          "price",
          product[:price],
          "created_at",
          product[:created_at],
          "comments_count",
          product[:comments_count]
      )
      end
    end
    
    cmt_query = <<SQL
SELECT product_id, name, content 
FROM 
(
  SELECT c2.id, row_num
  FROM
  (
    SELECT ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_at DESC) AS row_num, id
    FROM comments as c
    ORDER BY c.created_at DESC
  ) AS c2
  WHERE row_num < 6 
) AS c3
JOIN comments AS c4
  ON c3.id = c4.id
JOIN users as u
  ON c4.user_id = u.id
ORDER BY row_num
SQL
    comments = db.xquery(cmt_query).each 
    redis.multi do |multi|
      comments.each do |comment|
          comment[:content] = comment[:content].size > 25 ? comment[:content][0..24]+'…' : comment[:content]
          redis.lpush("#{LATEST_COMMENT_NAME_PREFIX}#{comment[:product_id]}", comment[:name])
          redis.lpush("#{LATEST_COMMENT_CONTENT_PREFIX}#{comment[:product_id]}", comment[:content])
      end
    end
    "Finish"
  end
end
