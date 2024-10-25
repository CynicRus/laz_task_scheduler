program laz_job_scheduler_sample;

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  Classes,
  SysUtils,
  job_scheduler { you can add units after this };

type

  { TJobTester }

  TJobTester = class
  public
    procedure TestRecurrentJob(Job: TSchedulerJob);
    procedure TestOneTimeJob(Job: TSchedulerJob);

    procedure OnJobStart(Sender: TObject; Job: TSchedulerJob);
    procedure OnJobExecute(Sender: TObject; Job: TSchedulerJob);
    procedure OnJobDone(Sender: TObject; Job: TSchedulerJob);
  end;


var
  Scheduler: TScheduler;
  Worker: TSchedulerWorker;
  Tester: TJobTester;

  { TJobTester }

  // Процедура для тестирования задачи
  procedure TJobTester.TestRecurrentJob(Job: TSchedulerJob);
  begin
    WriteLn('Executing recurrent job with ID: ', Job.ID);
    Sleep(500); // Имитация работы
  end;

  procedure TJobTester.TestOneTimeJob(Job: TSchedulerJob);
  begin
    WriteLn('Executing one time job with ID: ', Job.ID);
    Sleep(500); // Имитация работы
  end;


  // События
  procedure TJobTester.OnJobStart(Sender: TObject; Job: TSchedulerJob);
  begin
    WriteLn('Job started with ID: ', Job.ID);
  end;

  procedure TJobTester.OnJobExecute(Sender: TObject; Job: TSchedulerJob);
  begin
    WriteLn('Job executing with ID: ', Job.ID);
  end;

  procedure TJobTester.OnJobDone(Sender: TObject; Job: TSchedulerJob);
  begin
    WriteLn('Job done with ID: ', Job.ID);
  end;

begin
  Scheduler := TScheduler.Create;
  Worker := TSchedulerWorker.Create;
  Tester := TJobTester.Create;
  try
    // Подключаем события
    Worker.OnJobStart := @Tester.OnJobStart;
    Worker.OnJobExecute := @Tester.OnJobExecute;
    Worker.OnJobDone := @Tester.OnJobDone;

    Scheduler.Worker := Worker;
    Scheduler.SetEvent(1, 1, 1, @Tester.TestRecurrentJob);
    Scheduler.SetEvent(2, 0, 5, @Tester.TestOneTimeJob);

    // Активируем расписание
    Scheduler.Active := True;

    // Ждем выполнения
    ReadLn;
  finally
    // Чистим
    Tester.Free;
    Scheduler.Free;
    Worker.Free;
  end;
end.
