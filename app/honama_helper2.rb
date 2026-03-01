
require "digest"
require "json"

# store any info in a log files named timestamp and a unique digits
# remove entries older than some days
# log to different file everytime avoid bottle necks at parallel processes
def honama_helper_log( text, log_dir: "data/log", bucket_name: "honama" )
	puts "\r** #{ text }"
	t = Time.now.utc
	t_id = "#{ t.year }#{ t.month }"
	log = "#{ t.inspect.to_s.gsub(/\s+/,"_") }  #{ text.to_s.strip }"
	path = "#{ log_dir }/#{ t_id }/#{ get_uuid }"
	#honama_helper_store( path ){ log }
end

# get unique identifier as series of digits starting with time values
def get_uuid( len = 30 )
	return "#{ Time.now.utc.inspect.to_s.gsub(/[^0-9]/,"") }#{ rand 10**len }"[0...len]
end

# download the body of a https website and return it as string
def honama_helper_get_www( url, timeout = nil )
	uri = URI.parse(url)
	http = Net::HTTP.new(uri.host, uri.port)
	if timeout
		http.open_timeout = timeout
		http.read_timeout = timeout
	end
	if url[/^https\:\/\//]
		http.use_ssl = true
		http.verify_mode = OpenSSL::SSL::VERIFY_NONE  if url[/localhost/i] or url[/\d+\.\d+\.\d+\.\d+/i]
	end
	res = ""
	res = http.get(uri.request_uri).body.to_s  rescue ""
	return res
end

# please convert the following Ruby code to XXXXXXX with strictly the same logic and keep the same function names and the same comments and show me how to run it on Ubuntu Linux
# send data object in a post request from client side to server in JSON format
def honama_helper_post( data, url, flag_post: true, port: nil, content: nil, flag_error: false, ssl_version: nil )
	res = nil
	require "net/http"
	require "json"
	if flag_post
		uri = URI( url )
		url << "/"  if url[-1] != "/"
	else
		uri = URI( "#{ url }?#{ data.map{|k,v| [k,v].join("=") }.join("&") }" )
	end
	http = Net::HTTP.new( uri.host, ( port ? port : uri.port ))

	#http.open_timeout = 5     # seconds to wait for connection (TCP handshake)
	#http.read_timeout = 10    # seconds to wait for server response
	#http.write_timeout = 5    # (if supported) for sending request body

	if url.to_s[/^https/i]
		http.use_ssl = true
		http.ssl_version = ssl_version  if ssl_version
	end
	content = { "Content-Type" => "application/json" }  if not( content )
	if flag_post
		request = Net::HTTP::Post.new( uri.path == "" ? uri : uri.path )
	else
		request = Net::HTTP::Get.new( url )
	end
	content.each{|k,v|  request[k] = v  }
	request.body = data.to_json  if flag_post
	response = http.request( request )
	res = response.body
	res = ( JSON.parse( res )  rescue res )
	return res
end

# communicate with shopify api
#
# https://shopify.github.io/shopify-api-ruby/
# https://shopify.github.io/shopify-api-ruby/usage/graphql.html
#
# minimum curl call:
# curl -X POST https://honama7.myshopify.com/admin/api/2024-10/graphql.json -H 'Content-Type: application/json' -H 'X-Shopify-Access-Token: shpua_aed4c2bef9df216254a9c99cc44e7e47' -d '{ "query": "{ shop { name } }" }'
#
# get sales records with minimum call:
# curl -X POST https://honama7.myshopify.com/admin/api/2024-10/graphql.json -H 'Content-Type: application/json' -H 'X-Shopify-Access-Token: shpua_aed4c2bef9df216254a9c99cc44e7e47' -d '{ "query": "{ orders(first: 10) { edges { node { id }}}}" }'
#
# curl -X POST https://honama7.myshopify.com/admin/api/2024-10/graphql.json -H 'Content-Type: application/json' -H 'X-Shopify-Access-Token: shpua_aed4c2bef9df216254a9c99cc44e7e47' -d '{"query":"{ orders(first: 1, sortKey: CREATED_AT, reverse: true) { edges { node { id name createdAt totalPriceSet { shopMoney { amount currencyCode } } } } } }"}'
#
# get sales records:
# curl -X POST https://honama7.myshopify.com/admin/api/2024-10/graphql.json -H 'Content-Type: application/json' -H 'X-Shopify-Access-Token: shpua_aed4c2bef9df216254a9c99cc44e7e47' -d '{ "query": "{ orders(first: 250, query: \"created_at:>=2001-12-31\") { edges { node { id name createdAt lineItems(first: 100) { edges { node { product { id title } quantity originalUnitPriceSet { shopMoney { amount currencyCode } } } } } totalPriceSet { shopMoney { amount currencyCode } } } } } }" }'
#
# get sales records with unit cost values too:
# curl -X POST https://honama7.myshopify.com/admin/api/2024-10/graphql.json -H 'Content-Type: application/json' -H 'X-Shopify-Access-Token: shpua_aed4c2bef9df216254a9c99cc44e7e47' -d '{"query":"{ orders(first: 250, query: \"created_at:>=2001-12-31\") { edges { node { lineItems(first: 10) { edges { node { variant { id inventoryItem { unitCost { amount currencyCode } } } } } } } } } }"}'
#
# https://chatgpt.com/c/67515695-5cfc-8010-98fc-f3174b1ff72d
# gpt question:
# Please get the sales data with unit cost for all product variants using "hasNextPage" and "endCursor" to get all records for the last whole 1 year period in Ruby, but minimize the number of API calls by maximizing the number of records to retrieve at a time dynamically by checking how much a single query costs and calculate a safe number of records to retrieve, and deivide that number by 2 for safety.
#
# yyy
def honama_helper_shopify_api( shop, token, query, target = "graphql", flag_post: true, wait_count: 100, wait_error: 3 )
	res = nil
	url = "https://#{ shop }#{ API_URL }/#{ target }.json"
	content = {
		"Content-Type" => "application/json",
		"X-Shopify-Access-Token" => token,
	}
	# retry api call untill success
	#shop_name_real, shop2 = honama_helper_store_shop_name2( shop )
	wait_count.times {
		begin
			# flush stdout before the call so the stdout can appear in the logs
			# yyy
			$stdout.flush

			res = honama_helper_post( query, url, port: nil, content: content, flag_post: flag_post )
			break  if res

			#honama_helper_log( "shop #{ shop2 }  error  wait #{ wait_error } sec on api error..." )
			sleep wait_error

		rescue => e
			#honama_helper_log( "shop #{ shop2 }  error  wait #{ wait_error } sec on api error..." )
			#sleep wait_error

			# print errors
			error = "#{ e.message }  #{ e.backtrace }"
			honama_helper_log( "shop #{ shop }  error  #{ error }" )
			info = "error:  shop #{ shop }  #{ error }"
			honama_helper_notify( info )

			raise e

			res = nil
			break
		end
	}
	return res
end

