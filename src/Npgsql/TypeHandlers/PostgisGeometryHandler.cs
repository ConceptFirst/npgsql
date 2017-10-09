﻿#region License
// The PostgreSQL License
//
// Copyright (C) 2017 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.TypeHandling;
using Npgsql.TypeMapping;
using NpgsqlTypes;

namespace Npgsql.TypeHandlers
{
    /// <summary>
    /// Type Handler for the postgis geometry type.
    /// </summary>
    [TypeMapping("geometry", NpgsqlDbType.Geometry, new[]
    {
        typeof(PostgisGeometry),
        typeof(PostgisPoint),
        typeof(PostgisMultiPoint),
        typeof(PostgisLineString),
        typeof(PostgisMultiLineString),
        typeof(PostgisPolygon),
        typeof(PostgisMultiPolygon),
        typeof(PostgisGeometryCollection),
    })]
    class PostgisGeometryHandler : NpgsqlTypeHandler<PostgisGeometry>,
        INpgsqlTypeHandler<PostgisPoint>, INpgsqlTypeHandler<PostgisMultiPoint>,
        INpgsqlTypeHandler<PostgisLineString>, INpgsqlTypeHandler<PostgisMultiLineString>,
        INpgsqlTypeHandler<PostgisPolygon>, INpgsqlTypeHandler<PostgisMultiPolygon>,
        INpgsqlTypeHandler<PostgisGeometryCollection>,
        INpgsqlTypeHandler<byte[]>
    {
        [CanBeNull]
        readonly ByteaHandler _byteaHandler;

        public PostgisGeometryHandler()
        {
            _byteaHandler = new ByteaHandler();
        }

        #region Read

        public override async ValueTask<PostgisGeometry> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            await buf.Ensure(5, async);
            var bo = (ByteOrder)buf.ReadByte();
            var id = buf.ReadUInt32(bo);

            var srid = 0u;
            if ((id & (uint)EwkbModifiers.HasSRID) != 0)
            {
                await buf.Ensure(4, async);
                srid = buf.ReadUInt32(bo);
            }

            var geom = await DoRead(buf, (WkbIdentifier)(id & 7), bo, async);
            geom.SRID = srid;
            return geom;
        }

        async ValueTask<PostgisGeometry> DoRead(NpgsqlReadBuffer buf, WkbIdentifier id, ByteOrder bo, bool async)
        {
            switch (id)
            {
            case WkbIdentifier.Point:
                await buf.Ensure(16, async);
                return new PostgisPoint(buf.ReadDouble(bo), buf.ReadDouble(bo));

            case WkbIdentifier.LineString:
            {
                await buf.Ensure(4, async);
                var points = new Coordinate2D[buf.ReadInt32(bo)];
                for (var ipts = 0; ipts < points.Length; ipts++)
                {
                    await buf.Ensure(16, async);
                    points[ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                }
                return new PostgisLineString(points);
            }

            case WkbIdentifier.Polygon:
            {
                await buf.Ensure(4, async);
                var rings = new Coordinate2D[buf.ReadInt32(bo)][];

                for (var irng = 0; irng < rings.Length; irng++)
                {
                    await buf.Ensure(4, async);
                    rings[irng] = new Coordinate2D[buf.ReadInt32(bo)];
                    for (var ipts = 0; ipts < rings[irng].Length; ipts++)
                    {
                        await buf.Ensure(16, async);
                        rings[irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                    }
                }
                return new PostgisPolygon(rings);
            }

            case WkbIdentifier.MultiPoint:
            {
                await buf.Ensure(4, async);
                var points = new Coordinate2D[buf.ReadInt32(bo)];
                for (var ipts = 0; ipts < points.Length; ipts++)
                {
                    await buf.Ensure(21, async);
                    await buf.Skip(5, async);
                    points[ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                }
                return new PostgisMultiPoint(points);
            }

            case WkbIdentifier.MultiLineString:
            {
                await buf.Ensure(4, async);
                var rings = new Coordinate2D[buf.ReadInt32(bo)][];

                for (var irng = 0; irng < rings.Length; irng++)
                {
                    await buf.Ensure(9, async);
                    await buf.Skip(5, async);
                    rings[irng] = new Coordinate2D[buf.ReadInt32(bo)];
                    for (var ipts = 0; ipts < rings[irng].Length; ipts++)
                    {
                        await buf.Ensure(16, async);
                        rings[irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                    }
                }
                return new PostgisMultiLineString(rings);
            }

            case WkbIdentifier.MultiPolygon:
            {
                await buf.Ensure(4, async);
                var pols = new Coordinate2D[buf.ReadInt32(bo)][][];

                for (var ipol = 0; ipol < pols.Length; ipol++)
                {
                    await buf.Ensure(9, async);
                    await buf.Skip(5, async);
                    pols[ipol] = new Coordinate2D[buf.ReadInt32(bo)][];
                    for (var irng = 0; irng < pols[ipol].Length; irng++)
                    {
                        await buf.Ensure(4, async);
                        pols[ipol][irng] = new Coordinate2D[buf.ReadInt32(bo)];
                        for (var ipts = 0; ipts < pols[ipol][irng].Length; ipts++)
                        {
                            await buf.Ensure(16, async);
                            pols[ipol][irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                        }
                    }
                }
                return new PostgisMultiPolygon(pols);
            }

            case WkbIdentifier.GeometryCollection:
            {
                await buf.Ensure(4, async);
                var g = new PostgisGeometry[buf.ReadInt32(bo)];

                for (var i = 0; i < g.Length; i++)
                {
                    await buf.Ensure(5, async);
                    var elemBo = (ByteOrder)buf.ReadByte();
                    var elemId = (WkbIdentifier)(buf.ReadUInt32(bo) & 7);

                    g[i] = await DoRead(buf, elemId, elemBo, async);
                }
                return new PostgisGeometryCollection(g);
            }

            default:
                throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }

        ValueTask<byte[]> INpgsqlTypeHandler<byte[]>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
        {
            Debug.Assert(_byteaHandler != null);
            return _byteaHandler.Read(buf, len, async, fieldDescription);
        }

        #endregion Read

        #region Read concrete types

        async ValueTask<PostgisPoint> INpgsqlTypeHandler<PostgisPoint>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisPoint)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisMultiPoint> INpgsqlTypeHandler<PostgisMultiPoint>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisMultiPoint)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisLineString> INpgsqlTypeHandler<PostgisLineString>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisLineString)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisMultiLineString> INpgsqlTypeHandler<PostgisMultiLineString>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisMultiLineString)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisPolygon> INpgsqlTypeHandler<PostgisPolygon>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisPolygon)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisMultiPolygon> INpgsqlTypeHandler<PostgisMultiPolygon>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisMultiPolygon)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisGeometryCollection> INpgsqlTypeHandler<PostgisGeometryCollection>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeometryCollection)await Read(buf, len, async, fieldDescription);

        #endregion

        #region Write

        public override int ValidateAndGetLength(PostgisGeometry value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisPoint value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisMultiPoint value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisPolygon value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisMultiPolygon value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisLineString value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisMultiLineString value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisGeometryCollection value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(byte[] value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.Length;

        public override async Task Write(PostgisGeometry value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
        {
            // Common header
            if (value.SRID == 0)
            {
                if (buf.WriteSpaceLeft < 5)
                    await buf.Flush(async);
                buf.WriteByte(0); // We choose to ouput only XDR structure
                buf.WriteInt32((int)value.Identifier);
            }
            else
            {
                if (buf.WriteSpaceLeft < 9)
                    await buf.Flush(async);
                buf.WriteByte(0);
                buf.WriteInt32((int) ((uint)value.Identifier | (uint)EwkbModifiers.HasSRID));
                buf.WriteInt32((int) value.SRID);
            }

            switch (value.Identifier)
            {
            case WkbIdentifier.Point:
                if (buf.WriteSpaceLeft < 16)
                    await buf.Flush(async);
                var p = (PostgisPoint)value;
                buf.WriteDouble(p.X);
                buf.WriteDouble(p.Y);
                return;

            case WkbIdentifier.LineString:
                var l = (PostgisLineString)value;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async);
                buf.WriteInt32(l.PointCount);
                for (var ipts = 0; ipts < l.PointCount; ipts++)
                {
                    if (buf.WriteSpaceLeft < 16)
                        await buf.Flush(async);
                    buf.WriteDouble(l[ipts].X);
                    buf.WriteDouble(l[ipts].Y);
                }
                return;

            case WkbIdentifier.Polygon:
                var pol = (PostgisPolygon)value;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async);
                buf.WriteInt32(pol.RingCount);
                for (var irng = 0; irng < pol.RingCount; irng++)
                {
                    if (buf.WriteSpaceLeft < 4)
                        await buf.Flush(async);
                    buf.WriteInt32(pol[irng].Length);
                    for (var ipts = 0; ipts < pol[irng].Length; ipts++)
                    {
                        if (buf.WriteSpaceLeft < 16)
                            await buf.Flush(async);
                        buf.WriteDouble(pol[irng][ipts].X);
                        buf.WriteDouble(pol[irng][ipts].Y);
                    }
                }
                return;

            case WkbIdentifier.MultiPoint:
                var mp = (PostgisMultiPoint)value;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async);
                buf.WriteInt32(mp.PointCount);
                for (var ipts = 0; ipts < mp.PointCount; ipts++)
                {
                    if (buf.WriteSpaceLeft < 21)
                        await buf.Flush(async);
                    buf.WriteByte(0);
                    buf.WriteInt32((int)WkbIdentifier.Point);
                    buf.WriteDouble(mp[ipts].X);
                    buf.WriteDouble(mp[ipts].Y);
                }
                return;

            case WkbIdentifier.MultiLineString:
                var ml = (PostgisMultiLineString)value;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async);
                buf.WriteInt32(ml.LineCount);
                for (var irng = 0; irng < ml.LineCount; irng++)
                {
                    if (buf.WriteSpaceLeft < 9)
                        await buf.Flush(async);
                    buf.WriteByte(0);
                    buf.WriteInt32((int)WkbIdentifier.LineString);
                    buf.WriteInt32(ml[irng].PointCount);
                    for (var ipts = 0; ipts < ml[irng].PointCount; ipts++)
                    {
                        if (buf.WriteSpaceLeft < 16)
                            await buf.Flush(async);
                        buf.WriteDouble(ml[irng][ipts].X);
                        buf.WriteDouble(ml[irng][ipts].Y);
                    }
                }
                return;

            case WkbIdentifier.MultiPolygon:
                var mpl = (PostgisMultiPolygon)value;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async);
                buf.WriteInt32(mpl.PolygonCount);
                for (var ipol = 0; ipol < mpl.PolygonCount; ipol++)
                {
                    if (buf.WriteSpaceLeft < 9)
                        await buf.Flush(async);
                    buf.WriteByte(0);
                    buf.WriteInt32((int)WkbIdentifier.Polygon);
                    buf.WriteInt32(mpl[ipol].RingCount);
                    for (var irng = 0; irng < mpl[ipol].RingCount; irng++)
                    {
                        if (buf.WriteSpaceLeft < 4)
                            await buf.Flush(async);
                        buf.WriteInt32(mpl[ipol][irng].Length);
                        for (var ipts = 0; ipts < mpl[ipol][irng].Length; ipts++)
                        {
                            if (buf.WriteSpaceLeft < 16)
                                await buf.Flush(async);
                            buf.WriteDouble(mpl[ipol][irng][ipts].X);
                            buf.WriteDouble(mpl[ipol][irng][ipts].Y);
                        }
                    }
                }
                return;

            case WkbIdentifier.GeometryCollection:
                var coll = (PostgisGeometryCollection)value;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async);
                buf.WriteInt32(coll.GeometryCount);

                foreach (var x in coll)
                    await Write(x, buf, lengthCache, null, async);
                return;

            default:
                throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }

        public Task Write(PostgisPoint value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeometry)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisMultiPoint value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeometry)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisPolygon value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeometry)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisMultiPolygon value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeometry)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisLineString value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeometry)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisMultiLineString value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeometry)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisGeometryCollection value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeometry)value, buf, lengthCache, parameter, async);

        public Task Write(byte[] value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => _byteaHandler == null
                ? throw new NpgsqlException("Bytea handler was not found during initialization of PostGIS handler")
                : _byteaHandler.Write(value, buf, lengthCache, parameter, async);

        #endregion Write
    }


    /// <summary>
    /// Type Handler for the postgis geography type.
    /// </summary>
    [TypeMapping("geography", NpgsqlDbType.Geography, new[]
    {
        typeof(PostgisGeography),
        typeof(PostgisGeographyPoint),
        typeof(PostgisGeographyMultiPoint),
        typeof(PostgisGeographyLineString),
        typeof(PostgisGeographyMultiLineString),
        typeof(PostgisGeographyPolygon),
        typeof(PostgisGeographyMultiPolygon),
        typeof(PostgisGeographyCollection),
    })]
    class PostgisGeographyHandler : NpgsqlTypeHandler<PostgisGeography>,
        INpgsqlTypeHandler<PostgisGeographyPoint>, INpgsqlTypeHandler<PostgisGeographyMultiPoint>,
        INpgsqlTypeHandler<PostgisGeographyLineString>, INpgsqlTypeHandler<PostgisGeographyMultiLineString>,
        INpgsqlTypeHandler<PostgisGeographyPolygon>, INpgsqlTypeHandler<PostgisGeographyMultiPolygon>,
        INpgsqlTypeHandler<PostgisGeographyCollection>,
        INpgsqlTypeHandler<byte[]>
    {
        [CanBeNull]
        readonly ByteaHandler _byteaHandler;

        public PostgisGeographyHandler()
        {
            _byteaHandler = new ByteaHandler();
        }

        #region Read

        public override async ValueTask<PostgisGeography> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            await buf.Ensure(5, async);
            var bo = (ByteOrder)buf.ReadByte();
            var id = buf.ReadUInt32(bo);

            var srid = 0u;
            if ((id & (uint)EwkbModifiers.HasSRID) != 0)
            {
                await buf.Ensure(4, async);
                srid = buf.ReadUInt32(bo);
            }

            var geom = await DoRead(buf, (WkbIdentifier)(id & 7), bo, async);
            geom.SRID = srid;
            return geom;
        }

        async ValueTask<PostgisGeography> DoRead(NpgsqlReadBuffer buf, WkbIdentifier id, ByteOrder bo, bool async)
        {
            switch (id)
            {
                case WkbIdentifier.Point:
                    await buf.Ensure(16, async);
                    return new PostgisGeographyPoint(buf.ReadDouble(bo), buf.ReadDouble(bo));

                case WkbIdentifier.LineString:
                    {
                        await buf.Ensure(4, async);
                        var points = new Coordinate2D[buf.ReadInt32(bo)];
                        for (var ipts = 0; ipts < points.Length; ipts++)
                        {
                            await buf.Ensure(16, async);
                            points[ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                        }
                        return new PostgisGeographyLineString(points);
                    }

                case WkbIdentifier.Polygon:
                    {
                        await buf.Ensure(4, async);
                        var rings = new Coordinate2D[buf.ReadInt32(bo)][];

                        for (var irng = 0; irng < rings.Length; irng++)
                        {
                            await buf.Ensure(4, async);
                            rings[irng] = new Coordinate2D[buf.ReadInt32(bo)];
                            for (var ipts = 0; ipts < rings[irng].Length; ipts++)
                            {
                                await buf.Ensure(16, async);
                                rings[irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                            }
                        }
                        return new PostgisGeographyPolygon(rings);
                    }

                case WkbIdentifier.MultiPoint:
                    {
                        await buf.Ensure(4, async);
                        var points = new Coordinate2D[buf.ReadInt32(bo)];
                        for (var ipts = 0; ipts < points.Length; ipts++)
                        {
                            await buf.Ensure(21, async);
                            await buf.Skip(5, async);
                            points[ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                        }
                        return new PostgisGeographyMultiPoint(points);
                    }

                case WkbIdentifier.MultiLineString:
                    {
                        await buf.Ensure(4, async);
                        var rings = new Coordinate2D[buf.ReadInt32(bo)][];

                        for (var irng = 0; irng < rings.Length; irng++)
                        {
                            await buf.Ensure(9, async);
                            await buf.Skip(5, async);
                            rings[irng] = new Coordinate2D[buf.ReadInt32(bo)];
                            for (var ipts = 0; ipts < rings[irng].Length; ipts++)
                            {
                                await buf.Ensure(16, async);
                                rings[irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                            }
                        }
                        return new PostgisGeographyMultiLineString(rings);
                    }

                case WkbIdentifier.MultiPolygon:
                    {
                        await buf.Ensure(4, async);
                        var pols = new Coordinate2D[buf.ReadInt32(bo)][][];

                        for (var ipol = 0; ipol < pols.Length; ipol++)
                        {
                            await buf.Ensure(9, async);
                            await buf.Skip(5, async);
                            pols[ipol] = new Coordinate2D[buf.ReadInt32(bo)][];
                            for (var irng = 0; irng < pols[ipol].Length; irng++)
                            {
                                await buf.Ensure(4, async);
                                pols[ipol][irng] = new Coordinate2D[buf.ReadInt32(bo)];
                                for (var ipts = 0; ipts < pols[ipol][irng].Length; ipts++)
                                {
                                    await buf.Ensure(16, async);
                                    pols[ipol][irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                                }
                            }
                        }
                        return new PostgisGeographyMultiPolygon(pols);
                    }

                case WkbIdentifier.GeometryCollection:
                    {
                        await buf.Ensure(4, async);
                        var g = new PostgisGeography[buf.ReadInt32(bo)];

                        for (var i = 0; i < g.Length; i++)
                        {
                            await buf.Ensure(5, async);
                            var elemBo = (ByteOrder)buf.ReadByte();
                            var elemId = (WkbIdentifier)(buf.ReadUInt32(bo) & 7);

                            g[i] = await DoRead(buf, elemId, elemBo, async);
                        }
                        return new PostgisGeographyCollection(g);
                    }

                default:
                    throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }

        ValueTask<byte[]> INpgsqlTypeHandler<byte[]>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
        {
            Debug.Assert(_byteaHandler != null);
            return _byteaHandler.Read(buf, len, async, fieldDescription);
        }

        #endregion Read

        #region Read concrete types

        async ValueTask<PostgisGeographyPoint> INpgsqlTypeHandler<PostgisGeographyPoint>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeographyPoint)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisGeographyMultiPoint> INpgsqlTypeHandler<PostgisGeographyMultiPoint>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeographyMultiPoint)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisGeographyLineString> INpgsqlTypeHandler<PostgisGeographyLineString>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeographyLineString)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisGeographyMultiLineString> INpgsqlTypeHandler<PostgisGeographyMultiLineString>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeographyMultiLineString)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisGeographyPolygon> INpgsqlTypeHandler<PostgisGeographyPolygon>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeographyPolygon)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisGeographyMultiPolygon> INpgsqlTypeHandler<PostgisGeographyMultiPolygon>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeographyMultiPolygon)await Read(buf, len, async, fieldDescription);
        async ValueTask<PostgisGeographyCollection> INpgsqlTypeHandler<PostgisGeographyCollection>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeographyCollection)await Read(buf, len, async, fieldDescription);

        #endregion

        #region Write

        public override int ValidateAndGetLength(PostgisGeography value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisGeographyPoint value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisGeographyMultiPoint value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisGeographyPolygon value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisGeographyMultiPolygon value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisGeographyLineString value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisGeographyMultiLineString value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(PostgisGeographyCollection value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.GetLen(true);

        public int ValidateAndGetLength(byte[] value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => value.Length;

        public override async Task Write(PostgisGeography value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
        {
            // Common header
            if (value.SRID == 0)
            {
                if (buf.WriteSpaceLeft < 5)
                    await buf.Flush(async);
                buf.WriteByte(0); // We choose to ouput only XDR structure
                buf.WriteInt32((int)value.Identifier);
            }
            else
            {
                if (buf.WriteSpaceLeft < 9)
                    await buf.Flush(async);
                buf.WriteByte(0);
                buf.WriteInt32((int)((uint)value.Identifier | (uint)EwkbModifiers.HasSRID));
                buf.WriteInt32((int)value.SRID);
            }

            switch (value.Identifier)
            {
                case WkbIdentifier.Point:
                    if (buf.WriteSpaceLeft < 16)
                        await buf.Flush(async);
                    var p = (PostgisGeographyPoint)value;
                    buf.WriteDouble(p.X);
                    buf.WriteDouble(p.Y);
                    return;

                case WkbIdentifier.LineString:
                    var l = (PostgisGeographyLineString)value;
                    if (buf.WriteSpaceLeft < 4)
                        await buf.Flush(async);
                    buf.WriteInt32(l.PointCount);
                    for (var ipts = 0; ipts < l.PointCount; ipts++)
                    {
                        if (buf.WriteSpaceLeft < 16)
                            await buf.Flush(async);
                        buf.WriteDouble(l[ipts].X);
                        buf.WriteDouble(l[ipts].Y);
                    }
                    return;

                case WkbIdentifier.Polygon:
                    var pol = (PostgisGeographyPolygon)value;
                    if (buf.WriteSpaceLeft < 4)
                        await buf.Flush(async);
                    buf.WriteInt32(pol.RingCount);
                    for (var irng = 0; irng < pol.RingCount; irng++)
                    {
                        if (buf.WriteSpaceLeft < 4)
                            await buf.Flush(async);
                        buf.WriteInt32(pol[irng].Length);
                        for (var ipts = 0; ipts < pol[irng].Length; ipts++)
                        {
                            if (buf.WriteSpaceLeft < 16)
                                await buf.Flush(async);
                            buf.WriteDouble(pol[irng][ipts].X);
                            buf.WriteDouble(pol[irng][ipts].Y);
                        }
                    }
                    return;

                case WkbIdentifier.MultiPoint:
                    var mp = (PostgisGeographyMultiPoint)value;
                    if (buf.WriteSpaceLeft < 4)
                        await buf.Flush(async);
                    buf.WriteInt32(mp.PointCount);
                    for (var ipts = 0; ipts < mp.PointCount; ipts++)
                    {
                        if (buf.WriteSpaceLeft < 21)
                            await buf.Flush(async);
                        buf.WriteByte(0);
                        buf.WriteInt32((int)WkbIdentifier.Point);
                        buf.WriteDouble(mp[ipts].X);
                        buf.WriteDouble(mp[ipts].Y);
                    }
                    return;

                case WkbIdentifier.MultiLineString:
                    var ml = (PostgisGeographyMultiLineString)value;
                    if (buf.WriteSpaceLeft < 4)
                        await buf.Flush(async);
                    buf.WriteInt32(ml.LineCount);
                    for (var irng = 0; irng < ml.LineCount; irng++)
                    {
                        if (buf.WriteSpaceLeft < 9)
                            await buf.Flush(async);
                        buf.WriteByte(0);
                        buf.WriteInt32((int)WkbIdentifier.LineString);
                        buf.WriteInt32(ml[irng].PointCount);
                        for (var ipts = 0; ipts < ml[irng].PointCount; ipts++)
                        {
                            if (buf.WriteSpaceLeft < 16)
                                await buf.Flush(async);
                            buf.WriteDouble(ml[irng][ipts].X);
                            buf.WriteDouble(ml[irng][ipts].Y);
                        }
                    }
                    return;

                case WkbIdentifier.MultiPolygon:
                    var mpl = (PostgisGeographyMultiPolygon)value;
                    if (buf.WriteSpaceLeft < 4)
                        await buf.Flush(async);
                    buf.WriteInt32(mpl.PolygonCount);
                    for (var ipol = 0; ipol < mpl.PolygonCount; ipol++)
                    {
                        if (buf.WriteSpaceLeft < 9)
                            await buf.Flush(async);
                        buf.WriteByte(0);
                        buf.WriteInt32((int)WkbIdentifier.Polygon);
                        buf.WriteInt32(mpl[ipol].RingCount);
                        for (var irng = 0; irng < mpl[ipol].RingCount; irng++)
                        {
                            if (buf.WriteSpaceLeft < 4)
                                await buf.Flush(async);
                            buf.WriteInt32(mpl[ipol][irng].Length);
                            for (var ipts = 0; ipts < mpl[ipol][irng].Length; ipts++)
                            {
                                if (buf.WriteSpaceLeft < 16)
                                    await buf.Flush(async);
                                buf.WriteDouble(mpl[ipol][irng][ipts].X);
                                buf.WriteDouble(mpl[ipol][irng][ipts].Y);
                            }
                        }
                    }
                    return;

                case WkbIdentifier.GeometryCollection:
                    var coll = (PostgisGeographyCollection)value;
                    if (buf.WriteSpaceLeft < 4)
                        await buf.Flush(async);
                    buf.WriteInt32(coll.GeometryCount);

                    foreach (var x in coll)
                        await Write(x, buf, lengthCache, null, async);
                    return;

                default:
                    throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }

        public Task Write(PostgisGeographyPoint value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeography)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisGeographyMultiPoint value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeography)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisGeographyPolygon value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeography)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisGeographyMultiPolygon value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeography)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisGeographyLineString value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeography)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisGeographyMultiLineString value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeography)value, buf, lengthCache, parameter, async);

        public Task Write(PostgisGeographyCollection value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write((PostgisGeography)value, buf, lengthCache, parameter, async);

        public Task Write(byte[] value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => _byteaHandler == null
                ? throw new NpgsqlException("Bytea handler was not found during initialization of PostGIS handler")
                : _byteaHandler.Write(value, buf, lengthCache, parameter, async);

        #endregion Write
    }

}
