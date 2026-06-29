from django.urls import path

from .views_guitar import guitar_tuner


app_name = "intel"

urlpatterns = [
    path("guitar-tuner/", guitar_tuner, name="guitar_tuner"),
]
