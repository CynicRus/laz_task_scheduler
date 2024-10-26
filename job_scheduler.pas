{
BSD 3-Clause License

Copyright (c) 2010-2024, Aleksandr Vorobev
CynicRus@gmail.com

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
}

unit job_scheduler;

{$mode ObjFPC}{$H+}

interface

uses
  Classes, SysUtils, DateUtils
  {$IFDEF WINDOWS}
  , Windows
  {$ELSE}
   ,pthreads {$IFNDEF DARWIN}, Linux{$ELSE},Unix{$ENDIF}
  {$ENDIF}
  ;

type
  TJobType = (jtRecurring, jtOneTime);
  EJobProcException = Exception;
  TSchedulerWorker = class;
  TSchedulerJob = class;
  TSchedulerJobProc = procedure(Job: TSchedulerJob) of object;
  TJobEvent = procedure(Sender: TObject; Job: TSchedulerJob) of object;

  { TSchedulerJob }

  TSchedulerJob = class
  private
    FJobType: TJobType;
    FWorker: TSchedulerWorker;
    FJobProc: TSchedulerJobProc;
    FUserData: Pointer;
    {$IFDEF WINDOWS}
    FThreadHandle: THandle;
    {$ELSE}
    FThreadHandle: pthread_t;
    {$ENDIF}
    FID: integer;
    function GetTerminated: boolean;
  public
    constructor Create;
    destructor Destroy; override;
    property ID: integer read FID;
    property UserData: Pointer read FUserData write FUserData;
    property Worker: TSchedulerWorker read FWorker;
    property JobProc: TSchedulerJobProc read FJobProc;
    property JobType: TJobType read FJobType write FJobType;
    property Terminated: boolean read GetTerminated;
  end;

  { TSchedulerWorker }

  TSchedulerWorker = class
  private
    FJobs: TList;
    {$IFDEF WINDOWS}
    FMutex: THandle;
    FEvent: THandle;
    {$ELSE}
    FMutex: ppthread_mutex_t;
    FCondition: ppthread_cond_t;
    {$ENDIF}
    FActive: boolean;
    FOnJobStart: TJobEvent;
    FOnJobExecute: TJobEvent;
    FOnJobDone: TJobEvent;
    procedure Lock;
    procedure Unlock;
    procedure JobStart(Job: TSchedulerJob);
    procedure JobExecute(Job: TSchedulerJob);
    procedure JobDone(Job: TSchedulerJob);
    function GetCount: integer;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Stop(Wait: boolean = False);
    procedure JobAdd(JobID: integer; OnJob: TSchedulerJobProc;
      JobType: TJobType; UserData: Pointer = nil);
    function IsJob(JobID: integer): boolean;
    property Active: boolean read FActive write FActive;
    property Count: integer read GetCount;
    property OnJobStart: TJobEvent read FOnJobStart write FOnJobStart;
    property OnJobExecute: TJobEvent read FOnJobExecute write FOnJobExecute;
    property OnJobDone: TJobEvent read FOnJobDone write FOnJobDone;
  end;

  { TSchedulerEvent }

  TSchedulerEvent = class
  private
    FJobType: TJobType;
    FLastStart: int64;
    FNextStart: int64;
    FID: integer;
    FInterval: integer;
    FTimeOut: integer;
    FUserData: Pointer;
    FOnJob: TSchedulerJobProc;
    procedure SetInterval(const Value: integer);
    procedure SetTimeOut(const Value: integer);
  public
    constructor Create;
    destructor Destroy; override;
    property ID: integer read FID;
    property Interval: integer read FInterval write SetInterval;
    property TimeOut: integer read FTimeOut write SetTimeOut;
    property UserData: Pointer read FUserData write FUserData;
    property OnJob: TSchedulerJobProc read FOnJob write FOnJob;
    property JobType: TJobType read FJobType write FJobType;
  end;

  TScheduler = class(TThread)
  private
    FItems: TList;
    FActive: boolean;
    {$IFDEF WINDOWS}
    FMutex: THandle;
    FEvent: THandle;
    {$ELSE}
    FMutex: ppthread_mutex_t;
    FCondition: ppthread_cond_t;
    {$ENDIF}
    FWorker: TSchedulerWorker;
    procedure Lock;
    procedure Unlock;
    function Find(EventID: integer): TSchedulerEvent;
    procedure CheckEvents;
    function GetCount: integer;
    procedure SetActive(const Value: boolean);
  protected
    procedure Execute; override;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Clear;
    function SetEvent(EventID, Interval, TimeOut: integer;
      OnJob: TSchedulerJobProc; FirstStart: integer = -1): TSchedulerEvent;
    procedure ForceEvent(EventID: integer);
    property Active: boolean read FActive write SetActive;
    property Worker: TSchedulerWorker read FWorker write FWorker;
    property Count: integer read GetCount;
  end;

implementation

{$ifdef WINDOWS}
function JobProc(JobPtr: Pointer): ptrint;
{$ELSE}

function JobProc(JobPtr: Pointer): pointer; cdecl;
  {$ENDIF}
var
  Job: TSchedulerJob;
begin
  Job := TSchedulerJob(JobPtr);
  {$IFDEF WINDOWS}
  while Job.FThreadHandle = 0 do Sleep(1);
  CloseHandle(Job.FThreadHandle);
  {$ENDIF}

  with Job.Worker do
  begin
    try
      JobExecute(Job);
    except
      on E: Exception do
        EJobProcException.Create(E.Message);
    end;
    JobDone(Job);
  end;
  {$ifdef WINDOWS}
result := 0;
  {$ELSE}
  Result := nil;
  {$ENDIF}
end;

{ TSchedulerJob }

constructor TSchedulerJob.Create;
begin
  FID := 0;
  UserData := nil;
  FJobType := jtRecurring;
end;

destructor TSchedulerJob.Destroy;
begin
  inherited;
end;

function TSchedulerJob.GetTerminated: boolean;
begin
  Result := not Worker.Active;
end;

{ TSchedulerWorker }

constructor TSchedulerWorker.Create;
begin
  FActive := False;
  FJobs := TList.Create;

  {$IFDEF WINDOWS}
  FMutex := CreateMutex(nil, False, nil);
  FEvent := CreateEvent(nil, False, False, nil);
  {$ELSE}
  New(FMutex);
  New(FCondition);
  pthread_mutex_init(FMutex, nil);
  pthread_cond_init(FCondition, nil);
  {$ENDIF}
end;

destructor TSchedulerWorker.Destroy;
begin
  Stop(True);
  Lock;
  FJobs.Free;

  {$IFDEF WINDOWS}
  CloseHandle(FMutex);
  CloseHandle(FEvent);
  {$ELSE}
  pthread_mutex_destroy(FMutex);
  Dispose(FMutex);
  pthread_cond_destroy(FCondition);
  Dispose(FCondition);
  {$ENDIF}

  Unlock;
  inherited;
end;

function TSchedulerWorker.GetCount: integer;
begin
  Result := FJobs.Count;
end;

function TSchedulerWorker.IsJob(JobID: integer): boolean;
var
  I: integer;
begin
  Lock;
  Result := False;
  try
    for I := 0 to Count - 1 do
      if TSchedulerJob(FJobs[I]).ID = JobID then
      begin
        Result := True;
        Break;
      end;
  finally
    Unlock;
  end;
end;

procedure TSchedulerWorker.JobAdd(JobID: integer; OnJob: TSchedulerJobProc;
  JobType: TJobType; UserData: Pointer);
var
  Job: TSchedulerJob;
  {$IFDEF WINDOWS}
  ThID: Cardinal;
  {$ELSE}
  ThID: ppthread_t;
  {$ENDIF}
begin
  Lock;
  try
    if not Active then Exit;
    Job := TSchedulerJob.Create;
    Job.FID := JobID;
    Job.FUserData := UserData;
    Job.FWorker := Self;
    Job.FJobProc := OnJob;
    Job.FJobType := JobType;
    FJobs.Add(Job);
    JobStart(Job);
    {$IFDEF WINDOWS}
    ThID := 0;
    Job.FThreadHandle := BeginThread(nil, 0, @JobProc, Job, 0, ThID);
    {$ELSE}
    //ThID := nil;
    if pthread_create(ThID, nil, @JobProc, Job) <> 0 then
      raise Exception.Create('Unable to create thread');
    Job.FThreadHandle:= ThID^;
    {$ENDIF}
  finally
    Unlock;
  end;
end;

procedure TSchedulerWorker.JobDone(Job: TSchedulerJob);
var
  I: integer;
begin
  if Assigned(FOnJobDone) then
    FOnJobDone(Self, Job);

  Lock;
  try
    I := FJobs.IndexOf(Job);
    if I >= 0 then
    begin
      FJobs.Delete(I);
      Job.Free;
    end;
  finally
    Unlock;
  end;
end;

procedure TSchedulerWorker.JobStart(Job: TSchedulerJob);
begin
  if Assigned(FOnJobStart) then
    FOnJobStart(Self, Job);
end;

procedure TSchedulerWorker.JobExecute(Job: TSchedulerJob);
begin
  if Assigned(Job.JobProc) then
    Job.JobProc(Job);
end;

procedure TSchedulerWorker.Lock;
begin
  {$IFDEF WINDOWS}
  WaitForSingleObject(FMutex, INFINITE);
  {$ELSE}
  pthread_mutex_lock(FMutex);
  {$ENDIF}
end;

procedure TSchedulerWorker.Stop(Wait: boolean);
begin
  FActive := False;

  while Wait and (Count > 0) do
  begin
    {$IFDEF WINDOWS}
    WaitForSingleObject(FEvent, INFINITE);
    {$ELSE}
    pthread_cond_wait(FCondition, FMutex);
    {$ENDIF}
  end;
end;

procedure TSchedulerWorker.Unlock;
begin
  {$IFDEF WINDOWS}
  ReleaseMutex(FMutex);
  {$ELSE}
  pthread_mutex_unlock(FMutex);
  {$ENDIF}
end;

{ TSchedulerEvent }

constructor TSchedulerEvent.Create;
begin
  TimeOut := 0;
  FLastStart := 0;
  FID := 0;
  UserData := nil;
end;

destructor TSchedulerEvent.Destroy;
begin
  inherited;
end;

procedure TSchedulerEvent.SetInterval(const Value: integer);
begin
  if Value < 1 then
    FInterval := 1
  else
    FInterval := Value;
end;

procedure TSchedulerEvent.SetTimeOut(const Value: integer);
begin
  if Value < 0 then
    FTimeOut := 0
  else
    FTimeOut := Value;
end;

{ TScheduler }

constructor TScheduler.Create;
begin
  FItems := TList.Create;

  {$IFDEF WINDOWS}
  FMutex := CreateMutex(nil, False, nil);
  FEvent := CreateEvent(nil, False, False, nil);
  {$ELSE}
  New(FMutex);
  New(FCondition);
  pthread_mutex_init(FMutex, nil);
  pthread_cond_init(FCondition, nil);
  {$ENDIF}

  FreeOnTerminate := False;
  inherited Create(False);
end;

destructor TScheduler.Destroy;
begin
  Terminate;
  WaitFor;
  Clear;
  FItems.Free;

  {$IFDEF WINDOWS}
  CloseHandle(FMutex);
  CloseHandle(FEvent);
  {$ELSE}
  pthread_mutex_destroy(FMutex);
  Dispose(FMutex);
  pthread_cond_destroy(FCondition);
  Dispose(FCondition);
  {$ENDIF}

  inherited;
end;

procedure TScheduler.Clear;
var
  I: integer;
begin
  for I := 0 to Count - 1 do
    TSchedulerEvent(FItems[I]).Free;
  FItems.Clear;
end;

procedure TScheduler.CheckEvents;
var
  I: integer;
  Nt: int64;
  Event: TSchedulerEvent;
begin
  if Worker = nil then Exit;

  Lock;
  try
    I := 0;
    while I < Count do
    begin
      Nt := DateTimeToUnix(Now);
      Event := TSchedulerEvent(FItems[I]);

      case Event.JobType of
        jtRecurring:
        begin
          if (Nt >= Event.FNextStart) and (not Worker.IsJob(Event.ID)) then
          begin
            Event.FLastStart := Nt;
            Event.FNextStart := Event.FLastStart + Event.Interval;
            Worker.JobAdd(Event.ID, Event.OnJob, Event.JobType, Event.UserData);
          end;
        end;

        jtOneTime:
        begin
          if (Event.FNextStart = 0) then
            Event.FNextStart := Nt + Event.TimeOut;

          if (Nt >= Event.FNextStart) and (not Worker.IsJob(Event.ID)) then
          begin
            Worker.JobAdd(Event.ID, Event.OnJob, Event.JobType, Event.UserData);
            FItems.Delete(I);
            Event.Free;
            Continue;
          end;
        end;
      end;

      Inc(I);
    end;
  finally
    Unlock;
  end;
end;

procedure TScheduler.Execute;
{$IFDEF UNIX}
var
  timespec: ttimespec;
  {$IFDEF DARWIN}
  timeval: ttimeval;
  usec: Int64;
  {$ENDIF}
{$ENDIF}
begin
  repeat
    if Active then
      CheckEvents;

    {$IFDEF WINDOWS}
    if WaitForSingleObject(FEvent, 50) = WAIT_OBJECT_0 then
      Continue;
    {$ELSE}
    pthread_mutex_lock(FMutex);
    {$IFDEF DARWIN}
    fpgettimeofday(@timeval, nil);
    usec := timeval.tv_sec * 1000000 + timeval.tv_usec + 50 * 1000;
    timespec.tv_sec := usec div 1000000;
    timespec.tv_nsec := (usec mod 1000000) * 1000;

    if timespec.tv_nsec >= 1000000000 then
    begin
      timespec.tv_sec := timespec.tv_sec + 1;
      timespec.tv_nsec := timespec.tv_nsec - 1000000000;
    end;
    {$ELSE}
    clock_gettime(CLOCK_REALTIME, @timespec);
    timespec.tv_nsec := timespec.tv_nsec + 50 * 1000000;
    if timespec.tv_nsec >= 1000000000 then
    begin
      timespec.tv_sec := timespec.tv_sec + 1;
      timespec.tv_nsec := timespec.tv_nsec - 1000000000;
    end;
    {$ENDIF}

    pthread_cond_timedwait(FCondition, FMutex, @timespec);
    pthread_mutex_unlock(FMutex);
    {$ENDIF}

  until Terminated;
end;

function TScheduler.Find(EventID: integer): TSchedulerEvent;
var
  I: integer;
begin
  Lock;
  Result := nil;
  try
    for I := 0 to Count - 1 do
      if TSchedulerEvent(FItems[I]).ID = EventID then
      begin
        Result := TSchedulerEvent(FItems[I]);
        Break;
      end;
  finally
    Unlock;
  end;
end;

procedure TScheduler.ForceEvent(EventID: integer);
var
  Event: TSchedulerEvent;
  TimePast: integer;
begin
  Lock;
  try
    Event := Find(EventID);
    if Event = nil then Exit;

    if Event.TimeOut > 0 then
    begin
      TimePast := Abs(DateTimeToUnix(Now) - Event.FLastStart);
      if TimePast >= Event.TimeOut then
        Event.FNextStart := 0
      else
        Event.FNextStart := DateTimeToUnix(Now) + (Event.TimeOut - TimePast);
    end
    else
      Event.FNextStart := 0;
  finally
    Unlock;
  end;
end;

function TScheduler.GetCount: integer;
begin
  Result := FItems.Count;
end;

procedure TScheduler.Lock;
begin
  {$IFDEF WINDOWS}
  WaitForSingleObject(FMutex, INFINITE);
  {$ELSE}
  pthread_mutex_lock(FMutex);
  {$ENDIF}
end;

procedure TScheduler.SetActive(const Value: boolean);
begin
  Lock;
  try
    FActive := Value;
    if Assigned(FWorker) then
      FWorker.Active := Value;
  finally
    Unlock;
  end;
end;

function TScheduler.SetEvent(EventID, Interval, TimeOut: integer;
  OnJob: TSchedulerJobProc; FirstStart: integer): TSchedulerEvent;
begin
  Result := Find(EventID);
  Lock;
  try
    if Result = nil then
    begin
      Result := TSchedulerEvent.Create;
      Result.FID := EventID;
      FItems.Add(Result);
    end;
    Result.Interval := Interval;
    Result.TimeOut := TimeOut;
    Result.OnJob := OnJob;
    if Interval = 0 then
      Result.JobType := jtOneTime
    else
      Result.JobType := jtRecurring;

    if FirstStart < 0 then
    begin
      if Result.JobType = jtOneTime then
        Result.FNextStart := 0
      else
        Result.FNextStart := DateTimeToUnix(Now) + Interval;
    end
    else
      Result.FNextStart := DateTimeToUnix(Now) + FirstStart;
  finally
    Unlock;
  end;
end;

procedure TScheduler.Unlock;
begin
  {$IFDEF WINDOWS}
  ReleaseMutex(FMutex);
  {$ELSE}
  pthread_mutex_unlock(FMutex);
  {$ENDIF}
end;

end.
