using NCommonUtility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NCommonUtility
{

    public delegate void NSocketEventHandler(Object sender, NSocketEventArgs args);
    public class NSocketEventArgs : EventArgs
    {
        public NSocket Socket { get; private set; }
        public NSocketEventArgs(NSocket socket)
        {
            Socket = socket;
        }
    }

    public delegate void SendRecvEventHandler(Object sender, SendRecvEventArgs args);
    public class SendRecvEventArgs : EventArgs
    {
        public NSocket socket { get; private set; }
        public ByteArray dat { get; private set; }

        public SendRecvEventArgs(NSocket socket, ByteArray dat)
        {
            this.socket = socket;
            this.dat = dat;
        }
    }

    /// <summary>
    /// NSocketクラス
    /// </summary>
    public class NSocket
    {
        static public IPEndPoint GetIPEndPoint(string str_iaddr, string str_portno)
        {
            int portno;
            try
            {
                portno = int.Parse(str_portno);
            }
            catch (Exception e)
            {
                throw (new Exception("PortNo指定が不正です", e));
            }

            return GetIPEndPoint(str_iaddr, portno);
        }
        static public IPEndPoint GetIPEndPoint(string str_iaddr, int portno)
        {
            if (str_iaddr == "localhost")
            {
                return GetIPEndPoint(IPAddress.Loopback, portno);
            }
            IPAddress iaddr;
            try
            {
                iaddr = IPAddress.Parse(str_iaddr);
            }
            catch (Exception e)
            {
                throw (new Exception("IPAddress指定が不正です", e));
            }

            return GetIPEndPoint(iaddr, portno);
        }
        static public IPEndPoint GetIPEndPoint(IPAddress iaddr, int portno)
        {
            IPEndPoint endPoint = null;
            try
            {
                endPoint = new IPEndPoint(iaddr, portno);
            }
            catch (Exception e)
            {
                throw (new Exception($"IPEndPointが生成できません({e.Message})"));
            }
            return endPoint;
        }
        protected Socket _soc = null;
        protected IPEndPoint _self_EndPoint = null;     // バインドするためのEndPoint
        protected ByteArray _RecvBuff = null;
        private byte[] _recv_buf = null;
        private int _recv_len = 0;
        public int RecvBuffSize { 
            get { return _recv_buf.Length; }
        }

        public bool isOpen {  get { return _soc != null; } }

        private int _type = 0;
        public bool isServer { get { return _type == 1; } }
        public bool isClient { get { return _type == 2; } }


        public IPAddress LocalIPAddress { get { return ((IPEndPoint)_soc?.LocalEndPoint)?.Address; } }
        public int? LocalPortno { get { return ((IPEndPoint)_soc?.LocalEndPoint)?.Port; } }


        public IPAddress RemoteIPAddress { get { return ((IPEndPoint)_soc?.RemoteEndPoint)?.Address; } }
        public int? RemotePortno { get { return ((IPEndPoint)_soc?.RemoteEndPoint)?.Port; } }


        public event ThreadExceptionEventHandler OnExceptionEvent;
        public event NSocketEventHandler OnDisConnectEvent;
        public event NSocketEventHandler OnAcceptEvent;
        public event NSocketEventHandler OnConnectEvent;
        public event SendRecvEventHandler OnRecvEvent;
        public event SendRecvEventHandler OnSendEvent;

        public NSocket(int buff_size= 1024)
        {
            _recv_buf = new byte[buff_size];
            _RecvBuff = new ByteArray(_recv_buf);
            _type = 0;
        }
        protected NSocket(Socket soc, int buff_size = 1024)
        {
            // Acceptした時に生成されるサーバーソケット
            _soc = soc;
            _recv_buf = new byte[buff_size];
            _RecvBuff = new ByteArray(_recv_buf);
            _type = 1;
        }

        protected void OnException(Exception e)
        {
            if (OnExceptionEvent == null)
            {
                throw e;
            }
            OnExceptionEvent.Invoke(this, new ThreadExceptionEventArgs(e));
        }

        protected void OnDisConnect()
        {
            lock (this)
            {
                _soc?.Close();
                _soc = null;
            }
            OnDisConnectEvent?.Invoke(this, new NSocketEventArgs(this));
        }

        protected void OnAccept(NSocket socket)
        {
            OnAcceptEvent?.Invoke(this, new NSocketEventArgs(socket));
            // 受信スレッド起動
            socket.StartRecvThread();

            lock (this)
            {
                // Acceptの処理が終わったら引き続きAcceptを待つ
                _soc?.BeginAccept(new AsyncCallback(AcceptCallback), this);
            }

        }

        protected void OnConnect()
        {
            OnConnectEvent?.Invoke(this, new NSocketEventArgs(this));
            // 受信スレッド起動
            StartRecvThread();
        }

        /// <summary>
        /// 指定されたバッファにSocketから受信
        /// </summary>
        /// <param name="rbuff">受信バッファ</param>
        /// <param name="rsize">受信サイズ</param>
        /// <returns>実際に受信したサイズ</returns>
        /// <exception cref="EndOfStreamException"></exception>
        protected int Receive(byte[] rbuff, int rsize)
        {
            int rcnt = _soc.Receive(rbuff, _recv_len, rsize - _recv_len, SocketFlags.None);
            if (rcnt == 0)
            {
                throw new EndOfStreamException("データをすべて受信する前に、相手側が接続を閉じました。");
            }
            return rcnt;
        }

        /// <summary>
        /// 受信時の処理
        /// </summary>
        /// <remarks>受信スレッドのループから呼ばれる</remarks>
        /// <exception cref="OverflowException">受信バッファオーバーフロー</exception>
        protected virtual void OnRecv()
        {
            // _recv_bufに受信
            int rcnt = Receive(_recv_buf, _recv_buf.Length);
            _recv_len += rcnt;

            // 受信部分を解析してレコード長を取得
            int rsize = GetRecordLength(_RecvBuff, _recv_len);
            while (rsize != 0)
            {
                if (rsize < 0)
                {
                    // マイナスなら読み捨て
                    rsize = -rsize;
                    _RecvBuff.ShiftLeft(rsize);
                    _recv_len -= rsize;
                    if (_recv_len < 0)
                    {
                        _recv_len = 0;
                    }
                }
                else if (rsize > 0)
                {
                    // プラスなら1電文切り出し
                    ByteArray rec = GetOneRecv(rsize);
                    OnRecvEvent?.Invoke(this, new SendRecvEventArgs(this, rec));
                }
                // 残りの部分を解析
                rsize = GetRecordLength(_RecvBuff, _recv_len);
            }
            // rsizeが 0 なのに_recv_bufが一杯になったら例外(コンストラクタで十分な大きさを確保しておくこと)
            if (_recv_len >= _recv_buf.Length)
            {
                throw new OverflowException("受信バッファオーバーフロー");
            }
            // rsizeが 0 なら次の受信を待って_recv_bufに追加
        }

        /// <summary>
        /// レコード１件取得
        /// </summary>
        /// <remarks>受信バッファからレコードを１件取得する。まだ受信してない分は返却用のデータに直接受信する。</remarks>
        /// <param name="rsize">GetRecordLength() で判定したレコード長</param>
        /// <returns>1件分のレコードデータ</returns>
        private ByteArray GetOneRecv(int rsize)
        {
            // レコード長のバッファ確保
            byte[] rbuff = new byte[rsize];
            ByteArray rec = new ByteArray(rbuff);

            if (rsize > _recv_len)
            {
                // 追加の読み込みが必要ならコピーして残りをReceive()
                rec.Copy(_recv_buf, 0, _recv_len);
                while (rsize > _recv_len)
                {
                    int rcnt = Receive(rbuff, rsize);   // Receive()内でも_recv_lenを使う
                    _recv_len += rcnt;
                }
                // _recv_bufは既に全て取得済なので戻しておく
                _recv_len = 0;
            }
            else
            {
                // 全レコードが読み込み済ならばコピーしてシフト
                rec.Copy(_recv_buf, 0, rsize);
                _RecvBuff.ShiftLeft(rsize);
                _recv_len -= rsize;
            }
            return rec;
        }

        /// <summary>
        /// レコード長取得
        /// </summary>
        /// <remarks>
        /// 継承クラスで再定義することを前提としている。
        /// 例えば、ヘッダにデータ長があるのなら、ヘッダを読み込んだ時点でデータ長を返却する。
        /// ヘッダがまだ読み足りていなければ、0を返却する。
        /// 改行などのターミネーターでレコードを区切るのならば改行を検索して改行までの長さを返却する。
        /// 改行がみつからなければ、0を返却する。
        /// 先頭に特別な識別コードが必要ならば、該当の識別コードより前の長さをマイナスで返却すれば読み飛ばす、
        /// (次回に呼ばれた時、該当の識別コードから始まる状態で呼ばれる)
        /// </remarks>
        /// <param name="buff">受信バッファ</param>
        /// <param name="cnt">受信サイズ</param>
        /// <returns>判定したレコード長(マイナスの時は読み飛ばし、判定できなければ 0を返却)</returns>
        protected virtual int GetRecordLength(ByteArray buff, int cnt)
        {
            // デフォルトは受信した全部を１レコードとする
            return cnt;
        }

        protected virtual void OnSend(ByteArray buf)
        {
            OnSendEvent?.Invoke(this, new SendRecvEventArgs(this, buf));
        }

        public void SetSelfEndPoint(string iaddr, string portno)
        {
            _self_EndPoint = GetIPEndPoint(iaddr, portno);
        }
        public void SetSelfEndPoint(string iaddr, int portno = 0)
        {
            _self_EndPoint = GetIPEndPoint(iaddr, portno);
        }
        public void SetSelfEndPoint(IPAddress iaddr, int portno = 0)
        {
            if (!IPAddress.IsLoopback(iaddr))
            {
                IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
                if (!ipHostInfo.AddressList.Contains<IPAddress>(iaddr))
                {
                    throw new Exception("指定されたIPアドレスがホストに存在しません");
                }
            }
            _self_EndPoint = GetIPEndPoint(iaddr, portno);
        }

        public void Close()
        {
            lock (this)
            {
                _soc?.Close();
                _soc = null;
            }
        }

        public void Listen(string str_iaddr, string str_portno)
        {
            SetSelfEndPoint(str_iaddr, str_portno);
            Listen();
        }

        public void Listen(string str_iaddr, int portno)
        {
            SetSelfEndPoint(str_iaddr, portno);
            Listen();
        }

        public void Listen(IPAddress iaddr, int portno)
        {
            SetSelfEndPoint(iaddr, portno);
            Listen();
        }

        public void Listen()
        {
            lock (this)
            {
                bool needDisconnect = true;
                try
                {
                    if (_soc != null)
                    {
                        needDisconnect = false;
                        throw new Exception("既にListen中です");
                    }
                    _soc = new Socket(_self_EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    _soc.Bind(_self_EndPoint);
                    _soc.Listen(1);     // 一度に一個だけ受け付ける
                    _soc.BeginAccept(new AsyncCallback(AcceptCallback), this);
                }
                catch (Exception ex)
                {
                    if (needDisconnect)
                    {
                        OnDisConnect();
                    }
                    OnException(ex);
                }
            }
        }

        public void Connect(string str_iaddr, string str_portno)
        {
            IPEndPoint endPoint = GetIPEndPoint(str_iaddr, str_portno);
            Connect(endPoint);
        }
        public void Connect(string str_iaddr, int portno)
        {
            IPEndPoint endPoint = GetIPEndPoint(str_iaddr, portno);
            Connect(endPoint);
        }
        public void Connect(IPAddress iaddr, int portno)
        {
            IPEndPoint endPoint = GetIPEndPoint(iaddr, portno);
            Connect(endPoint);
        }
        public void Connect(IPEndPoint endPoint)
        {
            try
            {
                lock (this)
                {
                    if (_soc != null)
                    {
                        throw new Exception("Connect中にConnectしました");
                    }
                    _soc = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    if (_self_EndPoint != null)
                    {
                        _soc.Bind(_self_EndPoint);
                    }
                    _type = 2;
                    _soc.BeginConnect(endPoint, new AsyncCallback(ConnectCallback), this);
                }
            }
            catch (Exception ex)
            {
                OnDisConnect();
                OnException(new Exception("Connect処理でエラー発生", ex));
            }
        }


        protected virtual void AcceptCallback(IAsyncResult ar)
        {
            NSocket socket = (NSocket)ar.AsyncState;
            if (socket._soc == null)
            {
                return;
            }
            try
            {
                Socket soc = socket._soc.EndAccept(ar);
                OnAccept(new NSocket(soc));
            }
            catch (Exception ex)
            {
                OnDisConnect();
                OnException(ex);
            }
        }

        protected virtual void ConnectCallback(IAsyncResult ar)
        {
            NSocket socket = (NSocket)ar.AsyncState;
            if (socket._soc == null)
            {
                return;
            }
            try
            {
                socket._soc.EndConnect(ar);
                OnConnect();
            }
            catch (Exception ex)
            {
                OnDisConnect();
                OnException(new Exception("Connect中にエラー発生", ex));
            }
        }

        protected void StartRecvThread()
        {
            Task.Run(() => recvThread(this));
        }

        /// <summary>
        /// 受信スレッド
        /// </summary>
        /// <param name="_this"></param>
        static private void recvThread(NSocket _this)
        {
            try
            {
                while (true)
                {
                    if (_this._soc ==null ||_this._soc.Connected == false)
                    {
                        break;
                    }
                    _this.OnRecv();
                }
            }
            catch (Exception ex)
            {
                _this.OnException(ex);
            }
            finally
            {
                _this.OnDisConnect();
            }
        }

        public void Send(byte[] dat)
        {
            Send(new ByteArray(dat));
        }
        public void Send(ByteArray dat)
        {
            try
            {
                _soc?.BeginSend(dat.GetData(), 0, dat.Length(), SocketFlags.None, new AsyncCallback(SendCallback), (this, dat));
            }
            catch (Exception e)
            {
                OnDisConnect();
                OnException(new Exception("送信中に例外発生", e));
            }
        }

        protected virtual void SendCallback(IAsyncResult ar)
        {
            var (socket, dat) = ((NSocket, ByteArray))ar.AsyncState;
            if (socket._soc == null)
            {
                return;
            }
            try
            {
                socket._soc.EndSend(ar);
                OnSend(dat);
            }
            catch (Exception ex)
            {
                OnDisConnect();
                OnException(new Exception("ソケット送信エラー", ex));
            }
        }
    }
}
