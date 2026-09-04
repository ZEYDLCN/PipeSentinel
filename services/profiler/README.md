# services/profiler

Faz 1 kapsamında profiler mantığı bağımsız bir servis değil, modüler
monolith paketi `src/pipeline_sentinel/profiler.py` içinde yaşıyor
(bkz. mimari karar özeti §24: "MVP'de bileşenler modüler monolith olarak
çalıştırılabilir"). Ölçek büyüdüğünde bu dizin, kuyruk üzerinden
tetiklenen bağımsız bir worker'a çıkarılabilir.
