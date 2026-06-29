from django.shortcuts import render


def guitar_tuner(request):
    return render(request, "main/guitar_tuner.html")
