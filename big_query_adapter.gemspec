# coding: utf-8

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'big_query_adapter/version'

Gem::Specification.new do |spec|
  spec.name          = 'big_query_adapter'
  spec.version       = BigQueryAdapter::VERSION
  spec.authors       = ['pedrocarmona']
  spec.email         = ['pcarmona1990@gmail.com']

  spec.summary       = 'An ActiveRecord Google BigQuery adapter'
  spec.homepage      = 'https://github.com/pedrocarmona/big_query_adapter'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency 'google-cloud-bigquery', '~> 0.27.1'

  spec.add_development_dependency 'bundler', '~> 1.13'
  spec.add_development_dependency 'rake', '~> 12.0'
  spec.add_development_dependency 'rspec', '~> 3.0'
  spec.add_development_dependency 'rubocop', '~> 0.48'
  spec.add_development_dependency 'simplecov', '~> 0.14'
end
