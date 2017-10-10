#region License
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
    class PostgisGeometryHandler : PostgisHandler<PostgisGeometry>,
        INpgsqlTypeHandler<PostgisPoint>, INpgsqlTypeHandler<PostgisMultiPoint>,
        INpgsqlTypeHandler<PostgisLineString>, INpgsqlTypeHandler<PostgisMultiLineString>,
        INpgsqlTypeHandler<PostgisPolygon>, INpgsqlTypeHandler<PostgisMultiPolygon>,
        INpgsqlTypeHandler<PostgisGeometryCollection>
    {

        #region Template Methods

        protected override PostgisGeometry newPoint(double x, double y) => new PostgisPoint(x, y);
        protected override PostgisGeometry newLineString(Coordinate2D[] points) => new PostgisLineString(points);
        protected override PostgisGeometry newPolygon(Coordinate2D[][] rings) => new PostgisPolygon(rings);
        protected override PostgisGeometry newMultiPoint(Coordinate2D[] points) => new PostgisMultiPoint(points);
        protected override PostgisGeometry newMultiLineString(Coordinate2D[][] rings) => new PostgisMultiLineString(rings);
        protected override PostgisGeometry newMultiPolygon(Coordinate2D[][][] pols) => new PostgisMultiPolygon(pols);
        protected override PostgisGeometry newCollection(PostgisGeometry[] postGisTypes) => new PostgisGeometryCollection(postGisTypes);

        #endregion Template Methods

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

        protected override Coordinate2D decodePoint(PostgisGeometry geom)
            => ((PostgisPoint)geom).Coord;

        protected override Coordinate2D[] decodeLineString(PostgisGeometry geom)
            => ((PostgisLineString)geom).Points;

        protected override Coordinate2D[][] decodePolygon(PostgisGeometry geom)
            => ((PostgisPolygon)geom).Rings;

        protected override Coordinate2D[] decodeMultiPoint(PostgisGeometry geom)
            => ((PostgisMultiPoint)geom).Points;

        protected override Coordinate2D[][] decodeMultiLineString(PostgisGeometry geom)
            => ((PostgisMultiLineString)geom).Points;

        protected override Coordinate2D[][][] decodeMultiPolygon(PostgisGeometry geom)
            => ((PostgisMultiPolygon)geom).Points;

        protected override PostgisGeometry[] decodeCollection(PostgisGeometry geom)
            => ((PostgisGeometryCollection)geom).Geometries;

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

        #endregion Write
    }

}
