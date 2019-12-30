{% for post in site.posts %}
{{ post.date | date_to_string }}

# {{ post.title }}

{{ post.excerpt }}

<a href="{{ post.url }}">Continue...</a>
{% endfor %}
