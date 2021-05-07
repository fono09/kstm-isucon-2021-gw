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
  use Rack::Session::Redis, redis_server: 'redis://localhost:6379/', expires_in: 3600
  use Rack::Lineprof
  set :erb, escape_html: true
  set :public_folder, File.expand_path('../public', __FILE__)
  set :protection, true

  PRD_MAX_ID = 10000

  USER_ID_KEY_PREFIX = 'user_id_'
  USER_BOUGHT_PREFIX = 'user_bought_'

  PRODUCT_LATEST_COMMENTS_PREFIX = 'product_latest_comments_'
  PRODUCT_LATEST_COMMENTS_KEY_NAME = 'name'
  PRODUCT_LATEST_COMMENTS_KEY_CONTENT = 'content'

  PRODUCT_COMMENTS_COUNT_PREFIX = 'product_comments_count_'

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
      redis = Redis.new
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
      redis.multi do |multi|
        multi.incr("#{PRODUCT_COMMENTS_COUNT_PREFIX}#{product_id}")
        (3..0).each do |index|
          multi.rename(
            "#{PRODUCT_LATEST_COMMENTS_PREFIX}#{product_id}_#{index}",
            "#{PRODUCT_LATEST_COMMENTS_PREFIX}#{product_id}_#{index+1}"
          )
        end
        multi.hset(
          "#{PRODUCT_LATEST_COMMENTS_PREFIX}#{product_id}_0",
          PRODUCT_LATEST_COMMENTS_KEY_NAME,
          user_name,
          PRODUCT_LATEST_COMMENTS_KEY_CONTENT,
          content
        )
      end
      db.xquery('INSERT INTO comments (product_id, user_id, content, created_at) VALUES (?, ?, ?, ?)', product_id, user_id, content, time_now_db)
    end
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

    
    prd_query = <<SQL
SELECT
  id,
  name,
  description,
  image_path,
  price,
  created_at
FROM products
WHERE id > ?
  AND id <= ?
ORDER BY id DESC
SQL
    product_rows = db.xquery(prd_query, 
      PRD_MAX_ID - ((page + 1) * limit),
      PRD_MAX_ID - (page * limit),
    )

    product_ids = product_rows.map {|elem| elem[:id]}
    pcc_key = product_ids.map do |id|
      "#{PRODUCT_COMMENTS_COUNT_PREFIX}#{id}"
    end

    
    comments = {}
    comment_counts = []
      comment_counts = redis.mget(*pcc_key)
      product_comments = product_ids.map do |product_id|
        comment_list = []
        (0..4).each do |index|
          result = redis.hgetall("#{PRODUCT_LATEST_COMMENTS_PREFIX}#{product_id}_#{index}")
          comment_list.push({name: result["name"], content: result["content"]})
        end
      comments[product_id] = comment_list
    end

    products = (product_rows.zip(comment_counts)).map do |elem|
      elem[0][:comments_count] = elem[1]
      elem[0]
    end

    erb :index, locals: { products: products, comments: comments}
  end

  get '/users/:user_id' do
    cache_control :public
    products_query = <<SQL
SELECT p.id, p.name, p.description, p.image_path, p.price, h.created_at
FROM histories as h
LEFT OUTER JOIN products as p
ON h.product_id = p.id
WHERE h.user_id = ?
ORDER BY h.id DESC
SQL
    products = db.xquery(products_query, params[:user_id])

    total_pay = 0
    products.each do |product|
      total_pay += product[:price]
    end

    user = db.xquery('SELECT * FROM users WHERE id = ?', params[:user_id]).first
    erb :mypage, locals: { products: products, user: user, total_pay: total_pay }
  end

  get '/products/:product_id' do
    cache_control :public
    product = db.xquery('SELECT * FROM products WHERE id = ?', params[:product_id]).first
    comments = db.xquery('SELECT * FROM comments WHERE product_id = ?', params[:product_id])
    erb :product, locals: { product: product, comments: comments }
  end

  post '/products/buy/:product_id' do
    authenticated!
    buy_product(params[:product_id], current_user[:id])
    redirect "/users/#{current_user[:id]}"
  end

  post '/comments/:product_id' do
    authenticated!
    result = create_comment(params[:product_id], current_user[:id], current_user[:name], params[:content])
    redirect "/users/#{current_user[:id]}"
  end

  get '/initialize' do
    keys = redis.keys("#{USER_ID_KEY_PREFIX}*")
    keys.each do |key|
      redis.del(key)
    end

    keys = redis.keys("#{USER_BOUGHT_PREFIX}*")
    keys.each do |key|
      redis.del(key)
    end

    keys = redis.keys("#{PRODUCT_LATEST_COMMENTS_PREFIX}*")
    keys.each do |key|
      redis.del(key)
    end

    keys = redis.keys("#{PRODUCT_COMMENTS_COUNT_PREFIX}*")
    keys.each do |key|
      redis.del(key)
    end


    db.query('DELETE FROM users WHERE id > 5000')
    db.query('DELETE FROM products WHERE id > 10000')
    db.query('DELETE FROM comments WHERE id > 200000')
    db.query('DELETE FROM histories WHERE id > 500000')

    users = db.query('SELECT id, email FROM users')
    users.each do |user|
      redis.set("#{USER_ID_KEY_PREFIX}#{user[:id]}", user[:email])
    end

    pcc_query =<<SQL
SELECT product_id, COUNT(1) AS cnt
FROM comments
GROUP BY product_id
SQL
    product_comments_count = db.query(pcc_query)
    product_comments_count.each do |pcc|
      redis.set("#{PRODUCT_COMMENTS_COUNT_PREFIX}#{pcc[:product_id]}", pcc[:cnt])
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

    
    cmt_query = <<SQL
SELECT product_id, (row_num - 1) AS `index`, name, content 
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
SQL
    latest_comments = db.xquery(cmt_query)
    latest_comments.each do |comment|
       redis.hset(
        "#{PRODUCT_LATEST_COMMENTS_PREFIX}#{comment[:product_id]}_#{comment[:index]}",
        PRODUCT_LATEST_COMMENTS_KEY_NAME,
        comment[:name],
        PRODUCT_LATEST_COMMENTS_KEY_CONTENT,
        comment[:content]
      )
    end

    "Finish"
  end
end
